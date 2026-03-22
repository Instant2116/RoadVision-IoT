from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData


def process_agent_data(
    agent_data: AgentData,
) -> ProcessedAgentData:
    """
    Process agent data and classify the state of the road surface.
    Parameters:
        agent_data (AgentData): Agent data that containing accelerometer, GPS, and timestamp.
    Returns:
        ProcessedAgentData: Processed data containing the classified state of the road surface and agent data.
    """
    # Classify by accelerometer magnitude
    acc = agent_data.accelerometer
    magnitude = (acc.x**2 + acc.y**2 + acc.z**2) ** 0.5

    baseline = 16500
    deviation = abs(magnitude - baseline)

    if deviation > 7000:
        road_state = "pothole"
    elif deviation > 2500:
        road_state = "bump"
    else:
        road_state = "normal"
    return ProcessedAgentData(road_state=road_state, agent_data=agent_data)
