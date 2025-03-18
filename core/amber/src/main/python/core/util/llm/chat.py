import os
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline

# Force CPU usage
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# Retrieve the model path from the environment variable (if using local models)
model_path = os.getenv("MODEL_PATH", "AlgorithmicResearchGroup/flan-t5-base-arxiv-cs-ml-question-answering")

# Load model and tokenizer for Seq2Seq tasks
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForSeq2SeqLM.from_pretrained(model_path)  # Corrected to use Seq2Seq model
model.to(device)  # Move model to CPU

# Create a pipeline wrapper for easier usage
pipe = pipeline("summarization", model=model, tokenizer=tokenizer, device=-1)  # device=-1 ensures CPU usage

def ask_model(context, question):
    """
    Uses the Flan-T5 model to answer the given question based on the provided context.
    """
    prompt = f"Context: {context}\nQuestion: {question}\nAnswer:"
    inputs = tokenizer(prompt, return_tensors="pt").to(device)  # Move inputs to CPU
    outputs = model.generate(**inputs, max_length=200)
    return tokenizer.decode(outputs[0], skip_special_tokens=True)

# Debugging: Check PyTorch device info
if __name__ == "__main__":
    print(f"Running on device: {device}")
    context = "The sun is a star located at the center of the Solar System."
    question = "What is the sun?"
    print(ask_model(context, question))

    # Alternatively, using pipeline (higher-level API)
    print(pipe(f"Context: {context}\nQuestion: {question}"))