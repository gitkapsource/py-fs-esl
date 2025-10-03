from freeswitchESL import ESL
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# --- FreeSWITCH ESL Configuration ---
FS_HOST = "127.0.0.1"
FS_PORT = "8021"
FS_PASSWORD = "ClueCon" # Replace with your ESL password

# --- FastAPI Setup ---
app = FastAPI(
    title="FreeSWITCH Call API",
    description="An API to originate calls via FreeSWITCH ESL."
)

class CallRequest(BaseModel):
    phone_number: str
    caller_id: str = "1000" # Default caller ID if not provided

@app.post("/originate-call")
async def originate_call(request: CallRequest):
    """
    Initiates a new call through FreeSWITCH.
    """
    try:
        # Establish ESL connection
        esl_conn = ESL.ESLconnection(FS_HOST, FS_PORT, FS_PASSWORD)
        
        if not esl_conn.connected():
            raise RuntimeError("Could not connect to FreeSWITCH ESL.")

        # Construct the originate command.
        # This example uses a simple bridge to a destination.
        # The exact string will depend on your FreeSWITCH dialplan and setup.
        # This example assumes a SIP profile named 'internal'.
        originate_string = (
            f"bgapi originate {{origination_caller_id_number={request.caller_id}}}"
            f"sofia/internal/{request.phone_number} &park"
        )
        
        # Send the command to FreeSWITCH
        esl_conn.bgapi(originate_string)
        
        # FreeSWITCH ESL uses a background API, so we get an immediate response
        # about the command submission, not the call status itself.
        # For a full call flow, you would monitor events.
        return {
            "status": "success",
            "message": "Call originate command sent to FreeSWITCH.",
            "phone_number": request.phone_number,
            "caller_id": request.caller_id
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"An error occurred while trying to originate the call: {e}"
        )

# Health check endpoint
@app.get("/")
async def root():
    return {"message": "FastAPI is running."}
