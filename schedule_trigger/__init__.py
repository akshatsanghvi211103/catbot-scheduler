import azure.functions as func
import aiohttp
import asyncio
import logging
import os
import json
from datetime import datetime

# Configuration from environment variables
SCHEDULE_ENDPOINT = os.environ.get(
    "SCHEDULE_ENDPOINT", 
    "https://oncobot-h7fme6hue9f7buds.canadacentral-01.azurewebsites.net/schedule"
)
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "5"))

async def trigger_schedule():
    """
    Send POST request to /schedule endpoint to trigger background jobs.
    Returns a dictionary with the result status and details.
    """
    try:
        async with aiohttp.ClientSession() as session:
            logging.info(f"TRIGGER: Triggering schedule endpoint at {datetime.now()}")
            
            async with session.post(SCHEDULE_ENDPOINT) as response:
                if response.status == 202:
                    result = await response.json()
                    logging.info(f"SUCCESS: Schedule triggered successfully!")
                    if "executed_jobs" in result:
                        jobs = result.get("executed_jobs", [])
                        logging.info(f"   Executed jobs: {jobs}")
                        logging.info(f"   Current time: {result.get('current_time', 'Unknown')}")
                    else:
                        logging.info(f"   Response: {result}")
                    return {
                        "success": True,
                        "status": response.status,
                        "message": "Schedule triggered successfully",
                        "data": result
                    }
                elif response.status == 200:
                    result = await response.json()
                    logging.info(f"SUCCESS: Schedule checked - no jobs to run")
                    logging.info(f"   Message: {result.get('message', 'No message')}")
                    logging.info(f"   Current time: {result.get('current_time', 'Unknown')}")
                    return {
                        "success": True,
                        "status": response.status,
                        "message": "Schedule checked - no jobs to run",
                        "data": result
                    }
                else:
                    error_text = await response.text()
                    logging.error(f"ERROR: Schedule endpoint returned status: {response.status}")
                    logging.error(f"   Response: {error_text}")
                    return {
                        "success": False,
                        "status": response.status,
                        "message": f"HTTP {response.status}",
                        "error": error_text
                    }
                    
    except aiohttp.ClientError as e:
        logging.error(f"ERROR: Network error triggering schedule: {e}")
        return {
            "success": False,
            "status": None,
            "message": "Network error",
            "error": str(e)
        }
    except Exception as e:
        logging.error(f"ERROR: Unexpected error triggering schedule: {e}")
        return {
            "success": False,
            "status": None,
            "message": "Unexpected error",
            "error": str(e)
        }

async def trigger_schedule_with_retry():
    """
    Trigger the schedule endpoint with retry logic.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        logging.info(f"ATTEMPT: Attempt {attempt}/{MAX_RETRIES}")
        
        result = await trigger_schedule()
        
        if result["success"]:
            return result
        
        if attempt < MAX_RETRIES:
            logging.warning(f"WARNING: Attempt {attempt} failed, retrying in {RETRY_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RETRY_DELAY_SECONDS)
        else:
            logging.error(f"ERROR: All {MAX_RETRIES} attempts failed")
            return result
    
    return {"success": False, "message": "All retry attempts exhausted"}

def main(myTimer: func.TimerRequest) -> None:
    """
    Timer-triggered Azure Function that runs every hour.
    CRON expression "0 0 * * * *" means: run at the top of every hour.
    """
    utc_timestamp = datetime.utcnow().replace(tzinfo=None).isoformat()
    
    if myTimer.past_due:
        logging.warning('WARNING: Timer trigger function is running late!')
    
    logging.info(f'SCHEDULER: Expert reminder scheduler function executed at {utc_timestamp}')
    logging.info(f'   Target endpoint: {SCHEDULE_ENDPOINT}')
    
    # Run the async function
    try:
        result = asyncio.run(trigger_schedule_with_retry())
        
        if result["success"]:
            logging.info(f"SUCCESS: Schedule operation completed successfully")
        else:
            logging.error(f"ERROR: Schedule operation failed after retries: {result['message']}")
            
    except Exception as e:
        logging.error(f"ERROR: Fatal error in timer function: {e}")