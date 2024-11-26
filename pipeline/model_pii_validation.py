import openai

# Initialize the OpenAI client with your API key
client = openai.Client(api_key="sk-rc-31CcDP71_T-1uNVF8yFm7g", base_url="http://your-openai-server-url")

def construct_prompt_and_check_pii(context, pii_candidate, pii_type="PII", confirmTrue=False, window_size=30):
    tokens = context.split()
    try:
        pii_index = tokens.index(pii_candidate)
    except ValueError:
        truncated_context = context
    else:
        half_window = window_size // 2
        start_idx = max(0, pii_index - half_window)
        end_idx = min(len(tokens), pii_index + half_window + 1)
        truncated_context = " ".join(tokens[start_idx:end_idx])
    
    prompt = f"Your task is to classify [yes/no] if '{pii_candidate}' is an example of {pii_type} in the following context: '{truncated_context}'. Give a single word answer [yes/no]."

    res = client.chat.completions.create(
        model="meta-llama/Meta-Llama-3.1-70B-Instruct",
        messages=[
            {"content": prompt, "role": "user"}
        ],
        stream=False
    )

    model_response = res.choices[0].message.content.strip().lower()

    if "yes" in model_response and confirmTrue and len(context.split()) > window_size:
        full_prompt = f"Your task is to classify [yes/no] if '{pii_candidate}' is an example of {pii_type} in the full context: '{context}'."
        res_full = client.chat.completions.create(
            model="meta-llama/Meta-Llama-3.1-70B-Instruct",
            messages=[
                {"content": full_prompt, "role": "user"}
            ],
            stream=False
        )
        model_response_full = res_full.choices[0].message.content.strip().lower()
        return "yes" in model_response_full

    return "yes" in model_response
