import pandas as pd
import  re
from typing import List, Generator
from datatrove.pipeline.formatters.base import BaseFormatter
from datatrove.data import DocumentsPipeline, Document
from datatrove.utils.typeshelper import StatHints


class PIIContextExtractor(BaseFormatter):
    def __init__(self, eu_file_path, context_window=30, priorities_to_keep=['P0', 'P1', 'P2']): 
        """
        Initialize the PIIContextExtractor class with EU-specific regexes.

        eu_file_path: Path to the Excel file containing EU regex patterns and priorities.
        context_window: The number of words to capture around the PII entity (both before and after).
        """
        super().__init__()
        self.context_window = context_window
        self.eu_replacers = self.load_eu_regexes(eu_file_path)
        self.priorities_to_keep = priorities_to_keep

    def load_eu_regexes(self, eu_file_path):
        """
        Load and prepare EU-specific regex patterns from an Excel file.
        """
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
        """
        Detect PII candidates in the text using the EU-specific regex patterns.
        """
        detected_pii = []
        for matchType, priority, regex in self.eu_replacers:
            matches = re.finditer(regex, text)
            for match in matches:
                pii_candidate = match.group(0)
                start_idx, end_idx = match.span()
                detected_pii.append((matchType, priority, pii_candidate, start_idx, end_idx))
        
        return detected_pii

    def extract_context(self, text, start_idx, end_idx):
        """
        Extract context around the detected PII in terms of tokens, not characters.
        """
        tokens = text[:end_idx].split()
        match_token_index = len(tokens) - 1

        left_context_tokens = tokens[max(0, match_token_index - self.context_window // 2):match_token_index + 1]
        right_context_tokens = text[end_idx:].split()[:self.context_window // 2]
        left_context = ' '.join(left_context_tokens)
        right_context = ' '.join(right_context_tokens)

        return left_context + ' ' + right_context

    def format(self, text: str) -> List[dict]:
        """
        Process a document's text and return a list of contexts for each detected PII.
        """
        detected_pii = self.detect_pii(text)
        detected_pii = [pii for pii in detected_pii if pii[1] in self.priorities_to_keep]
        # sort by start index
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

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """
        Override run method to process multiple documents for each PII hit in the input documents.
        """
        for doc in data:
            self.stat_update(StatHints.total)
            with self.track_time():
                contexts = self.format(doc.text)
                for context_data in contexts:
                    new_doc = Document(
                        text=context_data["context"],
                        id=f"{doc.id}_pii_{context_data['pii_metadata']['pii_candidate']}",
                        metadata={**doc.metadata, "pii_metadata": context_data["pii_metadata"]}
                    )
                    yield new_doc