# 1. Base Image - גרסה קלה ורשמית של פייתון
FROM python:3.11-slim

# 2. הגדרת תיקיית העבודה בתוך הקונטיינר
WORKDIR /app
ENV PYTHONPATH=/app
# 3. העתקת קובץ הדרישות והתקנת הספריות
# אנחנו עושים את זה לפני העתקת שאר הקוד כדי לנצל את ה-Layer Cache של דוקר
COPY EventRecorder/requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt

# 4. העתקת כל שאר קבצי הפרויקט לתוך התיקייה
COPY ./EventRecorder ./EventRecorder
COPY ./shard ./shard

# 5. חשיפת הפורט (הצהרתי בלבד)
EXPOSE 8000

# 6. הפעלת השרת הסנכרוני
CMD ["python", "-m", "EventRecorder.main"]