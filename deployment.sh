# Deployment
# Register resource providers
az provider register --namespace 'Microsoft.Synapse' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Purview' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.MachineLearningServices' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.ContainerRegistry' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Network' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.DataShare' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Authorization' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.CognitiveServices' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.ManagedIdentity' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.KeyVault' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Storage' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.StreamAnalytics' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Devices' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Insights' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.EventHub' --consent-to-permissions --wait
az provider register --namespace 'Microsoft.DocumentDB' --consent-to-permissions --wait


# Create resource group
az group create -l westeurope -n <resource_group_name>

# Deploy
# Go to "Deploy" directory:
# Add ctrlDeploySampleArtifacts=true to populate sample data and notebooks, like this:

cd Deploy
az deployment group create --resource-group <resource_group_name> --template-file ./AzureAnalyticsE2E.bicep --parameters uniqueSuffix=<unique_suffix> synapseSqlAdminPassword=PasswordFor1stLogin! ctrlDeploySampleArtifacts=true

# Please, make sure you provide a uniqueSuffix parameter in case you are recreating infrastructure, so you would not have conflicts with deleted resources.

# Login
# To be able use SQL pools and run query you have set yourself as an admin.

# To get your user object id run:

az ad signed-in-user show --query id -o tsv

# And to assign an admin permission:

az synapse sql ad-admin create --workspace-name <synapse_workspace_name> --resource-group <resource_group_name> --display-name <email> --object-id 00000000-0000-0000-0000-000000000000