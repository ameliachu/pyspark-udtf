import difflib

from pyspark.sql.functions import udtf
from pyspark.sql.types import Row


@udtf(returnType="original: string, match: string, score: float")
class FuzzyMatch:
    """
    Finds the best match for a string from a list of candidates using Python's difflib.

    Input Arguments:
    - row (Row): A row containing the target string (1st col) and candidates list (2nd col).

    Output Columns:
    - original (str): The original target string.
    - match (str): The best matching string from the candidates.
    - score (float): A similarity score between 0.0 and 1.0.
    """
    def eval(self, row: Row):
        # Expecting row[0] to be target (str) and row[1] to be candidates (list)
        if not row or len(row) < 2:
            return

        target = row[0]
        candidates = row[1]

        if not target or not candidates:
            return

        # Find the best match
        matches = difflib.get_close_matches(target, candidates, n=1, cutoff=0.0)

        if matches:
            best_match = matches[0]
            # Calculate similarity score
            score = difflib.SequenceMatcher(None, target, best_match).ratio()
            yield target, best_match, float(score)
