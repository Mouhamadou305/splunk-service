package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Mouhamadou305/splunk-service/pkg/utils"
	cloudevents "github.com/cloudevents/sdk-go/v2" // make sure to use v2 cloudevents here
	"github.com/kelseyhightower/envconfig"
	keptnv2 "github.com/keptn/go-utils/pkg/lib/v0_2_0"
	logger "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const (
	sliFile                        = "splunk/sli.yaml"
	defaultSleepBeforeAPIInSeconds = 10
)

// We have to put a min of 60s of sleep for the splunk API to reflect the data correctly
// More info: https://github.com/Mouhamadou305/splunk-service/issues/8
var sleepBeforeAPIInSeconds int

func init() {
	var err error
	sleepBeforeAPIInSeconds, err = strconv.Atoi(strings.TrimSpace(os.Getenv("SLEEP_BEFORE_API_IN_SECONDS")))
	if err != nil || sleepBeforeAPIInSeconds < defaultSleepBeforeAPIInSeconds {
		logger.Infof("defaulting SLEEP_BEFORE_API_IN_SECONDS to 60s because it was set to '%v' which is less than the min allowed value of 10s", sleepBeforeAPIInSeconds)
		sleepBeforeAPIInSeconds = defaultSleepBeforeAPIInSeconds
	}
}

type splunkCredentials struct {
	Host  string `json:"host" yaml:"spHost"`
	Token string `json:"token" yaml:"spToken"`
	Port string `json:"port" yaml:"spPort"` 
}

// HandleGetSliTriggeredEvent handles get-sli.triggered events if SLIProvider == splunk
func HandleGetSliTriggeredEvent(ddKeptn *keptnv2.Keptn, incomingEvent cloudevents.Event, data *keptnv2.GetSLITriggeredEventData) error {
	var shkeptncontext string
	_ = incomingEvent.Context.ExtensionAs("shkeptncontext", &shkeptncontext)
	configureLogger(incomingEvent.Context.GetID(), shkeptncontext)

	logger.Infof("Handling get-sli.triggered Event: %s", incomingEvent.Context.GetID())

	// Step 1 - Do we need to do something?
	// Lets make sure we are only processing an event that really belongs to our SLI Provider
	if data.GetSLI.SLIProvider != "splunk" {
		logger.Infof("Not handling get-sli event as it is meant for %s", data.GetSLI.SLIProvider)
		return nil
	}

	// Step 2 - Send out a get-sli.started CloudEvent
	// The get-sli.started cloud-event is new since Keptn 0.8.0 and is required to be send when the task is started
	_, err := ddKeptn.SendTaskStartedEvent(data, ServiceName)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to send task started CloudEvent (%s), aborting...", err.Error())
		logger.Error(errMsg)
		return err
	}

	start, err := parseUnixTimestamp(data.GetSLI.Start)
	if err != nil {
		logger.Errorf("unable to parse sli start timestamp: %v", err)
		return err
	}
	end, err := parseUnixTimestamp(data.GetSLI.End)
	if err != nil {
		logger.Errorf("unable to parse sli end timestamp: %v", err)
		return err
	}

	// Step 4 - prep-work
	// Get any additional input / configuration data
	// - Labels: get the incoming labels for potential config data and use it to pass more labels on result, e.g: links
	// - SLI.yaml: if your service uses SLI.yaml to store query definitions for SLIs get that file from Keptn
	labels := data.Labels
	if labels == nil {
		labels = make(map[string]string)
	}

	// Step 5 - get SLI Config File
	// Get SLI File from splunk subdirectory of the config repo - to add the file use:
	//   keptn add-resource --project=PROJECT --stage=STAGE --service=SERVICE --resource=my-sli-config.yaml  --resourceUri=splunk/sli.yaml
	sliConfig, err := ddKeptn.GetSLIConfiguration(data.Project, data.Stage, data.Service, sliFile)
	logger.Infof("SLI config: %v", sliConfig)

	// FYI you do not need to "fail" if sli.yaml is missing, you can also assume smart defaults like we do
	// in keptn-contrib/dynatrace-service and keptn-contrib/prometheus-service
	if err != nil {
		// failed to fetch sli config file
		errMsg := fmt.Sprintf("Failed to fetch SLI file %s from config repo: %s", sliFile, err.Error())
		logger.Error(errMsg)
		// send a get-sli.finished event with status=error and result=failed back to Keptn

		_, err = ddKeptn.SendTaskFinishedEvent(&keptnv2.EventData{
			Status: keptnv2.StatusErrored,
			Result: keptnv2.ResultFailed,
			Labels: labels,
		}, ServiceName)

		return err
	}

	// Step 6 - do your work - iterate through the list of requested indicators and return their values
	// Indicators: this is the list of indicators as requested in the SLO.yaml
	// SLIResult: this is the array that will receive the results
	indicators := data.GetSLI.Indicators
	sliResults := []*keptnv2.SLIResult{}

	logger.Info("indicators:", indicators)
	errored := false

	for _, indicatorName := range indicators {
		// Pulling the data from Datadog api immediately gives incorrect data in api response
		// we have to wait for some time for the correct data to be reflected in the api response
		// TODO: Find a better way around the sleep time for splunk api
		logger.Infof("waiting for %vs so that the metrics data is reflected correctly in the api", sleepBeforeAPIInSeconds)
		time.Sleep(time.Second * time.Duration(sleepBeforeAPIInSeconds))

		query := replaceQueryParameters(data, sliConfig[indicatorName], start, end)
		logger.Infof("actual query sent to splunk: %v, from: %v, to: %v", query, start.Unix(), end.Unix())

		clusterConfig, err := rest.InClusterConfig()
		if err != nil {
			logger.Fatalf("unable to create kubernetes cluster config: %e", err)
		}

		kubeClient, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			logger.Fatalf("unable to create kubernetes client: %e", err)
		}

		// get splunk API URL for the provided Project from Kubernetes Config Map
		splunkCreds, err := getSplunkCredentials(data.Project, kubeClient.CoreV1())
		if err != nil {
			logger.Errorf("failed to get Splunk Credentials: %v", err.Error())
		}
		sliStartTime := strings.TrimSuffix(data.GetSLI.Start, "Z")
		sliEndTime := strings.TrimSuffix(data.GetSLI.End, "Z")
		logger.Infof("\nCCUSTOMQUERRY GO :  %v - \t %v \n", getMapContent(sliConfig), getMapContent(sliConfig))
		logger.Infof("\nMETRIC GO ou IndicatorName :  %v - \t %v \n", indicatorName, indicatorName)
		cmd := exec.Command("python", "-c", "import splunk;" + 
		"print(splunk.SplunkProvider(project='"+data.Project+
		"',stage='"+data.Stage+
		"',service='"+data.Service+
		"',labels="+getMapContent(data.Labels)+
		", customQueries=" + getMapContent(sliConfig)+
		", host='"+splunkCreds.Host+
		"', token='"+splunkCreds.Token+
		"', port='"+splunkCreds.Port+
		"').get_sli('"+indicatorName+"', '"+sliStartTime+"','"+sliEndTime+"'))")

		logger.Infof("command args: %v", cmd.Args)

		// cmd := exec.Command("python", "-c", "import splunk; print(splunk.SplunkProvider(project='test-splunk',stage='qa',service='helloservice', labels={}, customQueries={\"test_query\" : \"search |inputcsv test.csv | stats count\"}, host='ccf6-156-18-66-3.eu.ngrok.io', token='eyJraWQiOiJzcGx1bmsuc2VjcmV0IiwiYWxnIjoiSFM1MTIiLCJ2ZXIiOiJ2MiIsInR0eXAiOiJzdGF0aWMifQ.eyJpc3MiOiJhZG1pbiBmcm9tIGJhMzljNjk3ZTA5ZCIsInN1YiI6ImFkbWluIiwiYXVkIjoidGVzdCIsImlkcCI6IlNwbHVuayIsImp0aSI6IjU4MTRjNjBmNDNlNzk5ZDI1YzEzZDMyOWE4NTY2ZGM0ZmM5Mjg4MjQyMTg0NTAwMDY1NTdhYTYyYTI0YzYyNjQiLCJpYXQiOjE2Nzk0MTQxMjIsImV4cCI6MTY4MDQ1MDkyMiwibmJyIjoxNjc5NDE0MTIyfQ.gK2mdx7X8L6sdi50E0RvEI7wAvjEdq1P489pQ1isIRF5TzbL_RIXoB0Ku-zeRmo_Wc8hAcSNDPftu8QUuBCRkA', port=8000).get_sli('test_query', '2023-03-21T22:00:43.940','2023-03-21T22:02:50.940'))")
		
		out, err := cmd.CombinedOutput()

		sliValue, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)

		if err != nil {
			logger.Infof("\nOut :  %v - \t %v \n", string(out), sliValue)
			logger.Errorf("'%s': error getting value for the query: %v : %v\n", query, sliValue, err)
			errored = true
			continue
		}

		logger.Infof("response from the metrics api: %v, stringResp: %v", sliValue, string(out))

		if err != nil {
			sliResult := &keptnv2.SLIResult{
				Metric:  indicatorName,
				Value:   0,
				Success: false,
				Message: err.Error(),
			}
			sliResults = append(sliResults, sliResult)
			logger.WithFields(logger.Fields{"indicatorName": indicatorName}).Infof("got 0 in the SLI result (indicates empty response from the API)")
		} else {
			sliResult := &keptnv2.SLIResult{
				Metric:  indicatorName,
				Value:   sliValue,
				Success: true,
			}
			sliResults = append(sliResults, sliResult)
			logger.WithFields(logger.Fields{"indicatorName": indicatorName}).Infof("SLI result from the metrics api: %v", sliResult)
		}

	}

	// Step 7 - Build get-sli.finished event data
	getSliFinishedEventData := &keptnv2.GetSLIFinishedEventData{
		EventData: keptnv2.EventData{
			Status: keptnv2.StatusSucceeded,
			Result: keptnv2.ResultPass,
			Labels: labels,
		},
		GetSLI: keptnv2.GetSLIFinished{
			IndicatorValues: sliResults,
			Start:           data.GetSLI.Start,
			End:             data.GetSLI.End,
		},
	}

	if errored {
		getSliFinishedEventData.EventData.Status = keptnv2.StatusErrored
		getSliFinishedEventData.EventData.Result = keptnv2.ResultFailed
	}

	logger.Infof("SLI finished event: %v", *getSliFinishedEventData)

	_, err = ddKeptn.SendTaskFinishedEvent(getSliFinishedEventData, ServiceName)

	if err != nil {
		errMsg := fmt.Sprintf("Failed to send task finished CloudEvent (%s), aborting...", err.Error())
		logger.Error(errMsg)
		return err
	}

	return nil
}

func HandleConfigureMonitoringTriggeredEvent(ddKeptn *keptnv2.Keptn, incomingEvent cloudevents.Event, data *keptnv2.ConfigureMonitoringTriggeredEventData) error {
	var shkeptncontext string
	_ = incomingEvent.Context.ExtensionAs("shkeptncontext", &shkeptncontext)
	configureLogger(incomingEvent.Context.GetID(), shkeptncontext)

	logger.Infof("Handling configure-monitoring.triggered Event: %s", incomingEvent.Context.GetID())

	_, err := ddKeptn.SendTaskStartedEvent(data, ServiceName)
	if err != nil {
		logger.Errorf("err when sending task started the event: %v", err)
		return err
	}

	configureMonitoringFinishedEventData := &keptnv2.ConfigureMonitoringFinishedEventData{
		EventData: keptnv2.EventData{
			Status:  keptnv2.StatusSucceeded,
			Result:  keptnv2.ResultPass,
			Project: data.Project,
			Stage:   data.Service,
			Service: data.Service,
			Message: "Finished configuring monitoring",
		},
	}

	logger.Infof("Configure Monitoring finished event: %v", *configureMonitoringFinishedEventData)

	_, err = ddKeptn.SendTaskFinishedEvent(configureMonitoringFinishedEventData, ServiceName)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to send task finished CloudEvent (%s), aborting...", err.Error())
		logger.Error(errMsg)
		return err
	}

	return nil
}

func configureLogger(eventID, keptnContext string) {
	logger.SetFormatter(&utils.Formatter{
		Fields: logger.Fields{
			"service":      "splunk-service",
			"eventId":      eventID,
			"keptnContext": keptnContext,
		},
		BuiltinFormatter: &logger.TextFormatter{},
	})

	if os.Getenv(envVarLogLevel) != "" {
		logLevel, err := logger.ParseLevel(os.Getenv(envVarLogLevel))
		if err != nil {
			logger.WithError(err).Error("could not parse log level provided by 'LOG_LEVEL' env var")
		} else {
			logger.SetLevel(logLevel)
		}
	}
}

func replaceQueryParameters(data *keptnv2.GetSLITriggeredEventData, query string, start, end time.Time) string {
	query = strings.Replace(query, "$PROJECT", data.Project, -1)
	query = strings.Replace(query, "$STAGE", data.Stage, -1)
	query = strings.Replace(query, "$SERVICE", data.Service, -1)
	query = strings.Replace(query, "$project", data.Project, -1)
	query = strings.Replace(query, "$stage", data.Stage, -1)
	query = strings.Replace(query, "$service", data.Service, -1)
	durationString := strconv.FormatInt(getDurationInSeconds(start, end), 10)
	query = strings.Replace(query, "$DURATION", durationString, -1)
	return query
}

func getMapContent(mp map[string]string) string {
	dictn := "{"
	for key, element := range mp {
		dictn = dictn + "'" + key + "'" + " : " + "'" + element + "'" + ","
	}
	dictn = strings.TrimSuffix(dictn, ",") + "}"
	return dictn
}

func getDurationInSeconds(start, end time.Time) int64 {

	seconds := end.Sub(start).Seconds()
	return int64(math.Ceil(seconds))
}

func parseUnixTimestamp(timestamp string) (time.Time, error) {
	parsedTime, err := time.Parse(time.RFC3339, timestamp)
	if err == nil {
		return parsedTime, nil
	}

	timestampInt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return time.Now(), err
	}
	unix := time.Unix(timestampInt, 0)
	return unix, nil
}

var env utils.EnvConfig

// getSplunkCredentials fetches the splunk API URL for the provided project (e.g., from Kubernetes configmap)
func getSplunkCredentials(project string, kubeClient v1.CoreV1Interface) (*splunkCredentials, error) {

	if err := envconfig.Process("", &env); err != nil {
		logger.Error("Failed to process env var: " + err.Error())
	}

	logger.Info("Checking if external splunk instance has been defined for project " + project)

	secretName := "splunk-sli-splunk-service"

	secret, err := kubeClient.Secrets(env.PodNamespace).Get(context.TODO(), secretName, metav1.GetOptions{})

	// fallback: return cluster-internal splunk URL (configured via SplunkEndpoint environment variable)
	// in case no secret has been created for this project
	if err != nil {
		logger.Info("Could not retrieve or read secret (" + err.Error() + ") for project " + project + ". Using default: " + env.SplunkEndpoint)
		return nil, err //attention : ecouter audio
	}

	pc := splunkCredentials{}

	// Read Splunk config from Kubernetes secret as strings
	// Example: keptn create secret splunk-credentials-<project> --scope="keptn-splunk-sli-provider" --from-literal="SP_HOST=$SP_HOST"
	splunkHost, errHost := utils.ReadK8sSecretAsString(env.PodNamespace, secretName, "SP_HOST")
	splunkToken, errToken := utils.ReadK8sSecretAsString(env.PodNamespace, secretName, "SP_API_TOKEN")
	splunkPort, errPort := utils.ReadK8sSecretAsString(env.PodNamespace, secretName, "SP_PORT")

	if errHost == nil && errToken == nil  && errPort == nil {
		// found! using it
		pc.Host = strings.Replace(splunkHost, " ", "", -1)
		pc.Token = splunkToken
		pc.Port = splunkPort
		logger.Info("Successfully retrieved splunk credentials " + pc.Host + " and " + pc.Token + " and " + pc.Port)
	} else {
		// deprecated: try to use legacy approach
		err = yaml.Unmarshal(secret.Data["splunk-credentials"], &pc)

		if err != nil {
			logger.Info("Could not parse credentials for external splunk instance: " + err.Error())
			return nil, errors.New("invalid credentials format found in secret 'splunk-credentials-" + project)
		}

		// warn the user to migrate their credentials
		logger.Infof("Warning: Please migrate your splunk credentials for project %s. ", project)
		logger.Infof("See https://github.com/Mouhamadou305/splunk-sli-provider/issues/274 for more information.\n")
	}

	logger.Info("Using external splunk instance for project " + project + ": " + pc.Host)
	return &pc, nil
}
