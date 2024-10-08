import re
import pandas as pd
from datatrove.pipeline.formatters.base import BaseFormatter  

class PIIContextExtractor(BaseFormatter):
    def __init__(self, eu_file_path, context_window=30, priorities_to_keep=['P0', 'P1', 'P2']):
        super().__init__()
        self.context_window = context_window
        self.eu_replacers = self.load_eu_regexes(eu_file_path)
        self.priorities_to_keep = priorities_to_keep

    def load_eu_regexes(self, eu_file_path):
        df = pd.read_excel(eu_file_path)
        priority_order = pd.Categorical(df['Priority'], categories=['P0', 'P1', 'P2'], ordered=True)
        df['Priority'] = priority_order
        df = df.sort_values('Priority').reset_index(drop=True)

        whitespace_before = r"\b"
        whitespace_after = r"(\.|$|\,|\s)"

        replacers = []
        for _, row in df.iterrows():
            matchType = row['Identifier']
            priority = row['Priority']
            regex = whitespace_before + row['Regex'] + whitespace_after
            replacers.append((matchType, priority, regex))
        
        return replacers

    def detect_pii(self, text):
        detected_pii = []
        for matchType, priority, regex in self.eu_replacers:
            matches = re.finditer(regex, text)
            for match in matches:
                pii_candidate = match.group(0)
                start_idx, end_idx = match.span()
                detected_pii.append((matchType, priority, pii_candidate, start_idx, end_idx))
        
        return detected_pii

    def extract_context(self, text, start_idx, end_idx):
        tokens = text[:end_idx].split()
        match_token_index = len(tokens) - 1

        left_context_tokens = tokens[max(0, match_token_index - self.context_window // 2):match_token_index + 1]
        right_context_tokens = text[end_idx:].split()[:self.context_window // 2]
        left_context = ' '.join(left_context_tokens)
        right_context = ' '.join(right_context_tokens)

        return left_context + ' ' + right_context

    def format(self, text: str):
        detected_pii = self.detect_pii(text)
        detected_pii = [pii for pii in detected_pii if pii[1] in self.priorities_to_keep]
        detected_pii.sort(key=lambda x: x[3])

        context_documents = []
        for matchType, priority, pii_candidate, start_idx, end_idx in detected_pii:
            context = self.extract_context(text, start_idx, end_idx)
            context_documents.append({
                "context": context,
                "pii_metadata": {
                    'priority': priority,
                    'pii_candidate': pii_candidate,
                    'type': matchType
                }
            })
        return context_documents

    def run(self, data):
        for doc in data:
            contexts = self.format(doc['text'])
            for context_data in contexts:
                new_doc = {
                    'text': context_data["context"],
                    'id': f"{doc['id']}_pii_{context_data['pii_metadata']['pii_candidate']}",
                    'metadata': {**doc['metadata'], "pii_metadata": context_data["pii_metadata"]}
                }
                yield new_doc
