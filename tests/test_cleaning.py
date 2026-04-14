import pandas as pd

from nl_data_assistant.utils.cleaning import DataCleaner


def test_clean_dataframe_normalizes_headers_and_values():
    cleaner = DataCleaner()
    dataframe = pd.DataFrame(
        {
            " Student Name ": [" Ayush ", " Riya "],
            "CGPA": ["8.7", "9.1"],
            " ": [None, None],
        }
    )

    cleaned = cleaner.clean_dataframe(dataframe)

    assert list(cleaned.columns) == ["student_name", "cgpa"]
    assert cleaned.loc[0, "student_name"] == "Ayush"
    assert float(cleaned.loc[1, "cgpa"]) == 9.1
