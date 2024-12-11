# Ingest PCE events from Azure Storage into Splunk

This is an azure function app that is a queue based trigger, which listens to new blobs created on storage account where PCE writes events into.
When a new event is written in the form of a "blob", the function app picks it up, parses the events and with the context of a HEC configured on Splunk, transfers the events over to Splunk on the given sourcetype.

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fillumio-shield%2FAzureTalksToSplunk%2Frefs%2Fheads%2Fazure-to-splunk-queue-trigger-only%2Fazuredeploy.json)
