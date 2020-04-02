'''
/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
 '''
 
import argparse
import json
import logging
import time
 
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
 
createCertificateFromCsrRequestTopic = '$aws/certificates/create-from-csr/json'
createCertificateFromCsrAcceptedTopic = '$aws/certificates/create-from-csr/json/accepted'
createCertificateFromCsrRejectedTopic = '$aws/certificates/create-from-csr/json/rejected'
 
registerThingRequestTopicFormat = '$aws/provisioning-templates/{}/provision/json'
registerThingAcceptedTopicFormat = '$aws/provisioning-templates/{}/provision/json/accepted'
registerThingRejectedTopicFormat = '$aws/provisioning-templates/{}/provision/json/rejected'
 
 
class FleetProvisioningProcessor(object):
    def __init__(self, awsIoTMQTTClient, clientToken, templateName, templateParameters):
        self.clientToken = clientToken
        self.templateName = templateName
        self.templateParameters = templateParameters
        self.awsIoTMQTTClient = awsIoTMQTTClient
        self._setupCallbacks(self.awsIoTMQTTClient)
        self.createCertificateFromCsrResponse = None
        self.registerThingResponse = None
        self.failureResponse = None
        self.done = False
 
    def connect(self):
        self.awsIoTMQTTClient.connect()
 
    def disconnect(self):
        self.awsIoTMQTTClient.disconnect()
 
    def _setupCallbacks(self, awsIoTMQTTClient):
        print('Subscribing to topic: %s' % (createCertificateFromCsrAcceptedTopic))
        self.awsIoTMQTTClient.subscribe(createCertificateFromCsrAcceptedTopic, 1, self.createCertificateFromCsrCallback)
        print('Subscribing to topic: %s' % (createCertificateFromCsrRejectedTopic))
        self.awsIoTMQTTClient.subscribe(createCertificateFromCsrRejectedTopic, 1, self.failureCallback)
        registerThingAcceptedTopic = registerThingAcceptedTopicFormat.format(self.templateName)
        print('Subscribing to topic: %s' % (registerThingAcceptedTopic))
        self.awsIoTMQTTClient.subscribe(registerThingAcceptedTopic, 1, self.registerThingCallback)
        registerThingRejectedTopic = registerThingRejectedTopicFormat.format(self.templateName)
        print('Subscribing to topic: %s' % (registerThingRejectedTopic))
        self.awsIoTMQTTClient.subscribe(registerThingRejectedTopic, 1, self.failureCallback)
 
    def failureCallback(self, client, userdata, message):
        print("Received error from topic: " + message.topic)
        print(message.payload)
        self.failureResponse = json.loads(message.payload.decode('utf-8'))
 
    def _waitForCreateCertificateFromCsrResponse(self):
        # Wait for the response.
        loopCount = 0
        while loopCount < 10 and self.createCertificateFromCsrResponse is None:
            if self.createCertificateFromCsrResponse is not None or self.failureResponse is not None:
                break
            print('Waiting... createCertificateFromCsrResponse: ' + json.dumps(self.createCertificateFromCsrResponse))
            loopCount += 1
            time.sleep(1)
 
    def callCreateCertificateFromCsr(self, certificateCSR):
        createCertificateFromCsrRequest = {}
        createCertificateFromCsrRequest['certificateSigningRequest'] = certificateCSR
        createCertificateFromCsrRequestJson = json.dumps(createCertificateFromCsrRequest)
        self.awsIoTMQTTClient.publish(createCertificateFromCsrRequestTopic, createCertificateFromCsrRequestJson, 1)
        print('Published to topic %s: %s\n' % (createCertificateFromCsrRequestTopic, createCertificateFromCsrRequestJson))
        self._waitForCreateCertificateFromCsrResponse()
 
    def createCertificateFromCsrCallback(self, client, userdata, message):
        print("Received a new message from topic: " + message.topic)
        print(message.payload)
        self.createCertificateFromCsrResponse = json.loads(message.payload.decode('utf-8'))
 
    def _waitForRegisterThingResponse(self):
        # Wait for the response.
        loopCount = 0
        while loopCount < 10 and self.registerThingResponse is None:
            if self.registerThingResponse is not None or self.failureResponse is not None:
                break
            loopCount += 1
            print('Waiting... RegisterThingResponse: ' + json.dumps(self.registerThingResponse))
            time.sleep(1)
 
    def callRegisterThing(self):
        registerThingRequest = {}
        if self.createCertificateFromCsrResponse is None:
            raise Exception('createCertificateFromCsr API did not succeed')
        registerThingRequest['certificateOwnershipToken'] = self.createCertificateFromCsrResponse['certificateOwnershipToken']
 
        registerThingRequest['parameters'] = self.templateParameters
        registerThingRequestTopic = registerThingRequestTopicFormat.format(self.templateName)
        registerThingRequestJson = json.dumps(registerThingRequest)
        self.awsIoTMQTTClient.publish(registerThingRequestTopic, registerThingRequestJson, 1)
        print('Published topic %s: %s\n' % (registerThingRequestTopic, registerThingRequestJson))
        self._waitForRegisterThingResponse()
 
    def registerThingCallback(self, client, userdata, message):
        print("Received a new message from topic: " + message.topic)
        print(message.payload)
        self.registerThingResponse = json.loads(message.payload.decode('utf-8'))
        self.done = True
 
    def connectWithNewCertificate(self, rootCAPath):
        if self.createCertificateFromCsrResponse is None:
            raise Exception('createCertificateFromCsr API did not succeed.')
        filePrefix = self.createCertificateFromCsrResponse['certificateId'][:10]
        privateKeyFileName = filePrefix + '-private.pem.key'
        certificateFileName = filePrefix + '-certificate.pem.crt'
        self.textToFile(privateKeyFileName, self.createCertificateFromCsrResponse['privateKey'])
        self.textToFile(certificateFileName, self.createCertificateFromCsrResponse['certificatePem'])
        self.awsIoTMQTTClient.configureCredentials(rootCAPath, privateKeyFileName, certificateFileName)
        print('Connecting with new certificate ' + self.createCertificateFromCsrResponse['certificateId'])
 
        # Connecting with new credentials.
        self.awsIoTMQTTClient.connect()
 
        testTopic = 'topic/test'
        testMessage = 'Test Message'
        self.awsIoTMQTTClient.publish(testTopic, testTopic, 1)
        print('Published successfully to topic %s: %s\n' % (testTopic, testMessage))
 
    def textToFile(self, name, text):
        with open(name, "w+") as file:
            file.write(text)
 
    def isDone(self):
        return self.done
 
 
# Read in command-line parameters
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--endpoint", action="store", required=True, dest="host", help="Your AWS IoT custom endpoint")
parser.add_argument("-r", "--rootCA", action="store", required=True, dest="rootCAPath", help="Root CA file path")
parser.add_argument("-c", "--cert", action="store", dest="certificatePath", help="Certificate file path")
parser.add_argument("-k", "--key", action="store", dest="privateKeyPath", help="Private key file path")
parser.add_argument("-t", "--templateName", action="store", required=True, dest="templateName", help="Template name")
parser.add_argument("-tp", "--templateParameters", action="store", dest="templateParameters", help="Values for Template Parameters")
parser.add_argument("-p", "--port", action="store", dest="port", type=int, help="Port number override")
parser.add_argument("-w", "--websocket", action="store_true", dest="useWebsocket", default=False,
                    help="Use MQTT over WebSocket")
parser.add_argument("-id", "--clientId", action="store", dest="clientId", default="FleetProvisioningPythonSample",
                    help="Targeted client id")
parser.add_argument("-csr", "--csr", action="store", required=True, dest="certificateCSRPath", help="Certificate CSR Path")
 
args = parser.parse_args()
host = args.host
rootCAPath = args.rootCAPath
certificatePath = args.certificatePath
privateKeyPath = args.privateKeyPath
certificateCSR = open(args.certificateCSRPath, 'r').read()
clientId = args.clientId
port = args.port
useWebsocket = args.useWebsocket
templateName = args.templateName
templateParameters = json.loads(args.templateParameters)
 
 
if args.useWebsocket and args.certificatePath and args.privateKeyPath:
    parser.error("X.509 cert authentication and WebSocket are mutual exclusive. Please pick one.")
    exit(2)
 
if not args.useWebsocket and (not args.certificatePath or not args.privateKeyPath):
    parser.error("Missing credentials for authentication.")
    exit(2)
 
# Port defaults
if args.useWebsocket and not args.port:  # When no port override for WebSocket, default to 443
    port = 443
if not args.useWebsocket and not args.port:  # When no port override for non-WebSocket, default to 8883
    port = 8883
 
# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
 
# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
if useWebsocket:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath)
else:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)
 
# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
 
foundryProc = FleetProvisioningProcessor(myAWSIoTMQTTClient, clientId, templateName, templateParameters)
print('Starting provisioning...')
foundryProc.connect()
foundryProc.callCreateCertificateFromCsr(certificateCSR)
foundryProc.callRegisterThing()
foundryProc.disconnect()
if not foundryProc.isDone():
    raise Exception('Provisioning failed')
 
foundryProc.connectWithNewCertificate(rootCAPath)
foundryProc.disconnect()
