import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# Force CPU usage
device = torch.device("cpu")

# Retrieve the model path from the environment variable
model_path = os.getenv("MODEL_PATH")
if not model_path:
    raise EnvironmentError("MODEL_PATH environment variable is not set.")

# Load model and tokenizer with CPU-only setting
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForCausalLM.from_pretrained(model_path, device_map=None)  # Disable auto GPU mapping
model.to(device)  # Explicitly move the model to CPU

def ask_model(context, question):
    prompt = f"Here is the question context: {context}\nMy question is: {question}\nAnswer:"
    inputs = tokenizer(prompt, return_tensors="pt").to(device)  # Ensuring CPU usage
    outputs = model.generate(**inputs, max_length=200)
    return tokenizer.decode(outputs[0], skip_special_tokens=True)

# Debugging: Check PyTorch device info
if __name__ == "__main__":
    print(f"Running on device: {device}")
    context = "The sun is a star located at the center of the Solar System."
    question = "What is the sun?"
    print(ask_model(context, question))