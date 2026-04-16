# DataBuddy-Talk-2-DB-
Hi, This repository consists information about my recent project DataBuddy. In which the user can ask the questions in a human language and will get the response also  in the same way along with the SQL queries which was run in order to retrive the data and instant visualisations also like bar chart, pie chart, line chart, area chart etc
 
 
Adding more tabs in this PROJECT 
* CHAT UI
* PREDICTIONS
* ALERTS
* LIVE MONITORING 
* CRUD (Only Admin can do) 
 
  
  
    import time
import json
import uuid
import hashlib
import httpx

from backend.core.config import (
    LLM_BASE_URL,
    LLM_API_KEY,
    LLM_CLIENT_APP_NAME,
    LLM_REMOTE_USER,
    LLM_SERVICE,
    LLM_TEMPERATURE,
    LLM_MAX_RESPONSE_TOKENS,
    LLM_RESPONSE_FORMAT,
    LLM_REASONING_EFFORT,
    LLM_PERSONA,
    LLM_TIMEOUT_SECONDS,
    LLM_VERIFY_SSL,
    LLM_CA_BUNDLE,
)


def _current_timestamp_seconds() -> int:
    return int(time.time())


def _build_auth_headers(timestamp_seconds: int):
    """
    ST AI Bridge custom auth:
    stchatgpt-auth-nonce
    stchatgpt-auth-token = SHA1(clientAppName_service_apiKey_timestampSeconds_nonce)
    """
    nonce = str(uuid.uuid4())

    raw = f"{LLM_CLIENT_APP_NAME}_{LLM_SERVICE}_{LLM_API_KEY}_{timestamp_seconds}_{nonce}"
    sha1_token = hashlib.sha1(raw.encode("utf-8")).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "stchatgpt-auth-nonce": nonce,
        "stchatgpt-auth-token": sha1_token,
    }
    return headers


def _build_payload(prompt: str, timestamp_seconds: int):
    return {
        "version": 1,
        "clientAppName": LLM_CLIENT_APP_NAME,
        "timestamp": timestamp_seconds,   # IMPORTANT: seconds, not milliseconds
        "remoteUser": LLM_REMOTE_USER,
        "service": LLM_SERVICE,
        "temperature": LLM_TEMPERATURE,
        "maxResponseTokens": LLM_MAX_RESPONSE_TOKENS,
        "responseFormat": LLM_RESPONSE_FORMAT,
        "reasoningEffort": LLM_REASONING_EFFORT,
        "persona": LLM_PERSONA,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }


def _extract_text_from_response(data: dict) -> str:
    """
    Expected ST Chat Service response:
    {
      "service": "chat",
      "responseId": "...",
      "duration": 380,
      "completion": "..."
    }
    """
    error_code = data.get("errorCode")
    error_message = data.get("errorMessage") or data.get("message")

    if error_code not in (None, 0):
        raise RuntimeError(f"Gateway response: {error_message} | Full body: {data}")

    completion = data.get("completion")
    if isinstance(completion, str):
        return completion

    raise RuntimeError(f"Unexpected LLM response format: {data}")


def generate_sql_from_prompt(prompt: str, debug: bool = False) -> str:
    verify_value = LLM_CA_BUNDLE if LLM_CA_BUNDLE else LLM_VERIFY_SSL

    timestamp_seconds = _current_timestamp_seconds()
    headers = _build_auth_headers(timestamp_seconds)
    payload = _build_payload(prompt, timestamp_seconds)

    if debug:
        print("\n=== REQUEST URL ===")
        print(LLM_BASE_URL)
        print("\n=== REQUEST HEADERS ===")
        print(json.dumps(headers, indent=2))
        print("\n=== REQUEST PAYLOAD ===")
        print(json.dumps(payload, indent=2))

    with httpx.Client(
        timeout=LLM_TIMEOUT_SECONDS,
        verify=verify_value,
        trust_env=True,
    ) as client:
        response = client.post(
            LLM_BASE_URL,
            headers=headers,
            json=payload,
        )

        if debug:
            print("\n=== RESPONSE STATUS ===")
            print(response.status_code)
            print("\n=== RESPONSE TEXT ===")
            print(response.text)

        response.raise_for_status()
        data = response.json()

    return _extract_text_from_response(data)


def test_llm_connection() -> dict:
    try:
        resp = generate_sql_from_prompt("Return exactly this text: SELECT 1;", debug=True)
        return {
            "success": True,
            "response": resp,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }
   
     
  
   
  
