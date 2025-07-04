import json
import os
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from model.llm.suggestion import (
    SuggestionList,
    Suggestion,
    Changes,
    DataCleaningSuggestionList,
)
from model.web.input import SuggestionRequest, TableProfileSuggestionRequest
from model.llm.interpretation import InterpretationMethod
from suggestion_engine.generator import SuggestionGenerator

# Load environment variables
load_dotenv()

app = FastAPI(title="Texera Workflow Suggestion Service")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Initialize the suggestion generator with LLM support
suggestion_generator = SuggestionGenerator()

# @app.middleware("http")
# async def log_raw_body(request: Request, call_next):
#     body = await request.body()
#     print("🔥 RAW REQUEST BODY:", body.decode())
#     response = await call_next(request)
#     return response


@app.get("/")
async def root():
    return {"message": "Texera Workflow Suggestion Service is running"}


@app.post("/api/workflow-suggestion", response_model=SuggestionList)
async def generate_suggestions(request: SuggestionRequest):
    """
    Generate workflow suggestions based on the current workflow, compilation state, and execution state.
    """
    try:
        # Generate suggestions using the suggestion engine
        suggestions = suggestion_generator.generate_suggestions(request)

        return suggestions

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid workflow JSON format")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error generating suggestions: {str(e)}"
        )


@app.post("/api/data-cleaning-suggestion", response_model=DataCleaningSuggestionList)
async def generate_table_profile_suggestions(request: TableProfileSuggestionRequest):
    """
    Generate suggestions based on table profile and a target column.
    """
    try:
        suggestions = suggestion_generator.generate_data_cleaning_suggestions(request)
        return suggestions

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error generating suggestions: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9094)
