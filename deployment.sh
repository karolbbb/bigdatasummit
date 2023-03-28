# Deployment
# Register resource providers
az provider register --namespace 'Microsoft.Synapse' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Purview' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft. --accept-terms --consent-to-permissions --waitMachineLearningServices'
az provider register --namespace 'Microsoft.ContainerRegistry' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Network' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.DataShare' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Authorization' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.CognitiveServices' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.ManagedIdentity' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.KeyVault' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Storage' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.StreamAnalytics' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Devices' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.Insights' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.EventHub' --accept-terms --consent-to-permissions --wait
az provider register --namespace 'Microsoft.DocumentDB' --accept-terms --consent-to-permissions --wait


# Create resource group
az group create -l eastus -n e2e-synapse-deployment

# Deploy
# Go to "Deploy" directory:

cd Deploy
az deployment group create --resource-group e2e-synapse-deployment --template-file ./AzureAnalyticsE2E.bicep --parameters synapseSqlAdminPassword=PasswordFor1stLogin!

# Add ctrlDeploySampleArtifacts=true to populate sample data and notebooks, like this:

az deployment group create --resource-group e2e-synapse-deployment --template-file ./AzureAnalyticsE2E.bicep --parameters synapseSqlAdminPassword=PasswordFor1stLogin! ctrlDeploySampleArtifacts=true

# Please, make sure you provide a uniqueSuffix parameter in case you are recreating infrastructure, so you would not have conflicts with deleted resources.

# Login
# To be able use SQL pools and run query you have set yourself as an admin.

# To get your user object id run:

az ad signed-in-user show --query id -o tsv

# And to assign an admin permission:

az synapse sql ad-admin create --workspace-name SYNAPSE_WORKSPACE_NAME --resource-group e2e-synapse-deployment --display-name YOURACCOUNT@YOURDOMAIN --object-id 00000000-0000-0000-0000-000000000000