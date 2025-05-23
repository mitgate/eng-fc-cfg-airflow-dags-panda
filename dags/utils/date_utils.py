from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class DateUtils(object):
    def __init__(self, data):
        self.data = data
        
    def get_latest_date_parallel(self, date_column, date_format="%Y-%m-%dT%H:%M:%S", num_parts=10):
        """
        Identifies the largest date in a dictionary list based on the specified column,
        processing the data in parallel by splitting it into a specified number of parts.

        Args:
            data (list[dict]): List of dictionaries with the information.
            date_column (str): Name of the column that contains the dates.
            date_format (str): Format of the dates in the specified column.
            num_parts (int): Number of parts to divide the data for parallel processing.

        Returns:
            str: The largest date in the specified column.

        Date Formats:
            Below are some common date formats you can use:
            - "2024-12-04T23:15:00.000Z"          | Format: "%Y-%m-%dT%H:%M:%S.%fZ"
            - "2024-12-01T10:00:00"               | Format: "%Y-%m-%dT%H:%M:%S"
            - "2024-12-01 10:00:00"               | Format: "%Y-%m-%d %H:%M:%S"
            - "2024-12-01"                        | Format: "%Y-%m-%d"
            - "12/01/2024 10:00:00"               | Format: "%m/%d/%Y %H:%M:%S"
            - "01-12-2024 10:00:00"               | Format: "%d-%m-%Y %H:%M:%S"
            - "01/12/2024"                        | Format: "%d/%m/%Y"
            - "2024/12/01 10:00:00"               | Format: "%Y/%m/%d %H:%M:%S"
            - "01-Dec-2024 10:00:00"              | Format: "%d-%b-%Y %H:%M:%S"
            - "December 1, 2024 10:00 AM"         | Format: "%B %d, %Y %I:%M %p"
            - "Sun, 01 Dec 2024 10:00:00 GMT"     | Format: "%a, %d %b %Y %H:%M:%S %Z"

        Examples:
            data = [
                {"id": 1, "date": "2024-12-01T10:00:00.000Z", "value": 100},
                {"id": 2, "date": "2024-12-03T15:00:00.000Z", "value": 150},
            ]
            date_column = "date"
            date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
            num_parts = 5
            result = get_latest_date_parallel(data, date_column, date_format, num_parts)
        """
        data = self.data
        if not data or date_column not in data[0]:
            raise ValueError("❌ Invalid data or column not found.")
        
        if num_parts < 1:
            raise ValueError("❌ The number of parts must be at least 1.")

        # Function to find the largest date in a chunk of data
        def find_max_in_chunk(chunk):
            return max(chunk, key=lambda x: datetime.strptime(x[date_column], date_format))[date_column]

        # Split data into `num_parts` equal parts
        chunk_size = len(data) // num_parts + (len(data) % num_parts > 0)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        # Process chunks in parallel
        with ThreadPoolExecutor() as executor:
            max_dates = list(executor.map(find_max_in_chunk, chunks))

        # Find the largest date from the results of all chunks
        final_max_date = max(max_dates, key=lambda x: datetime.strptime(x, date_format))

        return final_max_date