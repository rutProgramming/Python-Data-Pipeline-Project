import multiprocessing
from collections import Counter
import heapq
import os

def read_file_chunks(file_path, chunk_size):
    with open(file_path, "r", encoding="utf-8") as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def process_chunk(chunk):
    counts = Counter()
    for line in chunk:
        if "Error: " in line:
            parts = line.split("Error: ")
            if len(parts) == 2:
                error = parts[1].strip()
                counts[f"Error: {error}"] += 1
    return counts

def combine_counters(results):
    final = Counter()
    for res in results:
        final.update(res)
    return final

def main():
    file_path = "./files/log.txt"
    chunk_size = 10000
    top_n = 5

    if not os.path.exists(file_path):
        print("File does not exist.")
        return

    chunks = read_file_chunks(file_path, chunk_size)

    with multiprocessing.Pool() as pool:
        results = pool.map(process_chunk, chunks)

    final_counter = combine_counters(results)

    top_errors = heapq.nlargest(top_n, final_counter.items(), key=lambda x: x[1])
    
    print(f"\nTop {top_n} most common error codes:")
    for error, count in top_errors:
        print(f"{error}: {count}")
    
if __name__ == "__main__":
    main()

# ניתוח סיבוכיות זמן ומקום 

# הגדרות בסיסיות:
# - m = מספר השורות בקובץ
# - c = גודל כל chunk 
# - k = מספר התהליכים 
# - n = מספר השגיאות הנפוצות שאנחנו מחפשים (top-n)
# - e = מספר שגיאות ייחודיות שנמצאו בפועל

# סיבוכיות זמן 

# 1. קריאת קובץ: O(m)
#   - הקריאה נעשית שורה-שורה ומחולקת ל־chunks
#
# 2. עיבוד כל chunk ב־multiprocessing: O(m)
#   - כל שורה נסרקת, נספרות שגיאות
#
# 3. מיזוג כל ה־Counter-ים: O(n)
#   - מאחדים את המילונים מכל התהליכים
#
# 4. מיון עם heapq כדי להוציא top-n: O(e * log(n))
#   - בניית heap בגודל n מתוך e

# סך הכל בזמן:
# במקרה גרוע: O(m + m + m + m* log(n)) = O(m + n)-e≈m
# במקרה ממוצע: O(m + e) ≈ O(m) כי בדר"כ e << m

# סיבוכיות מקום 

# 1. מרחב עבור chunks: O(c)
#   - chunk בודד כל פעם
#
# 2. תהליכי עיבוד: O(k * c)
#   - כל אחד מחזיק chunk בזיכרון
#
# 3. תוצר סופי (Counter): O(e)
#   - סופר את כל סוגי השגיאות
#
# 4. Heap ל־top-n: O(n)
#   - שומר רק את n הכי נפוצים

# סך הכל במקום:
# במקרה גרוע: O(k * c + e + n) → O(m + n)-e≈m
# במקרה ממוצע (מעט שגיאות): O(k * c + e + n) → O(e + n)
