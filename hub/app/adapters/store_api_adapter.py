import logging
from typing import List

import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway

logger = logging.getLogger(__name__)


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]) -> bool:
        """
        Save the processed road data to the Store API.
        Parameters:
            processed_agent_data_batch: List of processed road data to be saved.
        Returns:
            True if the data is successfully saved, False otherwise.
        """
        if not processed_agent_data_batch:
            return True
        url = f"{self.api_base_url.rstrip('/')}/processed_agent_data/"
        payload = [item.model_dump(mode="json") for item in processed_agent_data_batch]
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code in (200, 201):
                return True
            logger.warning(
                "Store API returned %s: %s", response.status_code, response.text
            )
            return False
        except requests.RequestException as e:
            logger.exception("Failed to save batch to Store API: %s", e)
            return False
