import os
from Deprecated_Azure_Pipeline.function_app import (
    ADLSManager, CosmosManager, SyncOrchestrator, JobMatcher,
    ADLS_ACCOUNT, ADLS_KEY, COSMOS_ENDPOINT, COSMOS_KEY,
    COSMOS_DB, COSMOS_CONTAINER, EMAIL_DIR, LINKEDIN_PATH
)

if __name__ == "__main__":
    adls = ADLSManager(ADLS_ACCOUNT, ADLS_KEY, "deepstatedatabase")
    cosmos = CosmosManager(COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DB, COSMOS_CONTAINER)
    cosmos.init_container()

    matcher = JobMatcher(tolerance_days=5, threshold=0.72)
    orchestrator = SyncOrchestrator(adls, cosmos, matcher)
    orchestrator.run()