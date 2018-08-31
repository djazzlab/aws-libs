# Boto3 requirements
from boto3 import client as boto3_cli

# Python 3 libraries requirements
from time import time as epoch_ts

#
# LOGGER class to work with AWS cloudwatch
#
class logger:
    # Properties
    logGroupName = ''
    logStreamName = ''
    sequenceToken = ''
    cwLogsClient = None

    # Constructor -- initialization in one call
    def __init__(self, logGroupName = None, logStreamName = None):
        # Load the CW Logs client
        self.cwLogsClient = boto3_cli('logs')
        
        if logGroupName is not None:
            self.setLogGroupName(logGroupName)
            
            if logStreamName is not None:
                self.setLogStreamName(logGroupName, logStreamName)

    # Methods
    def sendLogMessage(self, logMessage):
        # Write log message in CW log stream
        try:
            kwargs = dict(
                logGroupName = self.logGroupName,
                logStreamName = self.logStreamName,
                logEvents = [
                    {
                        'timestamp': int(epoch_ts() * 1000),
                        'message': logMessage
                    }
                ]
            )

            if self.sequenceToken is not None:
                kwargs['sequenceToken'] = self.sequenceToken

            respPutEvent = self.cwLogsClient.put_log_events(**kwargs)
            
            if 'nextSequenceToken' in respPutEvent:
                self.sequenceToken = respPutEvent['nextSequenceToken']
        except Exception as e:
            raise e

    def setLogGroupName(self, logGroupName):
        self.logGroupName = '/aws/lambda/{}'.format(logGroupName)

        # Check if the log group exists, if not create it
        if len(self.cwLogsClient.describe_log_groups(logGroupNamePrefix = self.logGroupName)['logGroups']) == 0:
            try:
                self.cwLogsClient.create_log_group(logGroupName = self.logGroupName)
            except Exception as e:
                raise e

    def setLogStreamName(self, logGroupName, logStreamName):
        self.logStreamName = logStreamName

        # Check if the log stream exists, it not create it
        logStreams = self.cwLogsClient.describe_log_streams(logGroupName = self.logGroupName, logStreamNamePrefix = self.logStreamName)['logStreams']
        if len(logStreams) == 0:
            self.sequenceToken = None
            try:
                self.cwLogsClient.create_log_stream(logGroupName = self.logGroupName, logStreamName = self.logStreamName)
            except Exception as e:
                raise e
        else:
            self.sequenceToken = None
            if 'uploadSequenceToken' in logStreams[0]:
                self.sequenceToken = logStreams[0]['uploadSequenceToken']                
