import asyncio
from contextlib import asynccontextmanager
from fastapi.responses import StreamingResponse

import uuid
from freeswitchESL import ESL
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# --- FreeSWITCH ESL Configuration ---
FS_HOST = "127.0.0.1"
FS_PORT = "8021"
FS_PASSWORD = "ClueCon" # Replace with your ESL password

class CallRequest(BaseModel):
    phone_number: str
    caller_id: str = "1000" # Default caller ID if not provided


# This queue will hold events coming from FreeSWITCH
event_queue = asyncio.Queue()
esl_conn = None

async def event_listener():
    """Background task to receive events from FreeSWITCH and put them in a queue."""
    global esl_conn
    while esl_conn.connected:
        try:
            # recvEvent() is a blocking call, so we must run it in a thread pool
            # to avoid blocking the event loop.
            event = await asyncio.to_thread(esl_conn.recvEvent)
            if event:
                await event_queue.put(event.serialize("json"))
        except Exception as e:
            print(f"ESL event listener error: {e}")
            break


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events to manage the ESL connection.
    """
    global esl_conn
    print("Attempting to connect to FreeSWITCH...")
    try:
        esl_conn = ESL.ESLconnection(FS_HOST, FS_PORT, FS_PASSWORD)
        if esl_conn.connected:
            print("Connected to FreeSWITCH. Subscribing to events.")
            
            # Subscribe to the specific events you need, such as CHANNEL_EXECUTE,
            # CHANNEL_HANGUP, and CUSTOM for call tracking.
            esl_conn.events("json", "ALL")
            
            # Start the background task to listen for events
            asyncio.create_task(event_listener())
        else:
            print("Failed to connect to FreeSWITCH.")
            esl_conn = None
    except Exception as e:
        print(f"Could not establish ESL connection: {e}")
        esl_conn = None
    
    yield  # The app will run here
    
    # On shutdown, clean up the connection
    if esl_conn:
        print("Disconnecting from FreeSWITCH.")
        esl_conn.disconnect()

# --- FastAPI Setup ---
app = FastAPI(
    lifespan=lifespan,
    title="FreeSWITCH Call API",
    description="An API to originate calls via FreeSWITCH ESL."
)

@app.post("/originate-call")
async def originate_call(request: CallRequest):
	"""
	Initiates a new call through FreeSWITCH.
	"""
	try:

		# Create a new UUID for the call's originating leg
		origination_uuid = str(uuid.uuid4())

		# Establish ESL connection
		#esl_conn = ESL.ESLconnection(FS_HOST, FS_PORT, FS_PASSWORD)

		global esl_conn
		if not esl_conn or not esl_conn.connected:
       			 raise HTTPException(status_code=500, detail="ESL connection is not active.")
 
		# Construct the originate command.
		# This example uses a simple bridge to a destination.
		# The exact string will depend on your FreeSWITCH dialplan and setup.
		# This example assumes a SIP profile named 'internal'.

		originate_string = (
			f"bgapi originate {{origination_uuid={origination_uuid},origination_caller_id_number={request.caller_id}}}"
			f"sofia/internal/{request.phone_number}%34.228.63.97 &park"
		)

		# Send the command to FreeSWITCH
		response = await asyncio.to_thread(esl_conn.bgapi, originate_string)

		# Check if response is None, which indicates a command failure
		if response is None:
			raise Exception("FreeSWITCH did not return a valid response for bgapi.")

		reply_text = response.getHeader("Reply-Text")
	
		# Check if the 'Reply-Text' header is missing or empty
		if not reply_text:
			raise Exception(f"FreeSWITCH reply text was empty. Full response headers: {response.headers}")


		# FreeSWITCH ESL uses a background API, so we get an immediate response
		# about the command submission, not the call status itself.
		# For a full call flow, you would monitor events.

		# Parse the response to get the job ID.
		#job_uuid = None

		# Check for a successful reply containing a Job-UUID
		if "Job-UUID:" in reply_text:
			job_uuid = reply_text.split("Job-UUID:")[1].strip()
			return {"status": "Call initiated", "job_uuid": job_uuid}
		else:
			raise Exception(f"FreeSWITCH error: {reply_text}")
	
	except Exception as e:
		raise HTTPException(
		status_code=500, 
		detail=f"An error occurred while trying to originate the call: {e}"
		)

# Health check endpoint
@app.get("/")
async def root():
    return {"message": "FastAPI is running."}


async def event_generator():
    """Generator to yield events for Server-Sent Events."""
    while True:
        event_data = await event_queue.get()
        yield f"data: {event_data}\n\n"

@app.get("/events/")
async def stream_events():
    """Endpoint for clients to subscribe to a stream of FreeSWITCH events."""
    return StreamingResponse(event_generator(), media_type="text/event-stream")

