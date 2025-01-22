# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

COPY . .

# remove stuffs
RUN rm -Rf app/data
RUN rm -Rf app/notebooks
RUN rm -Rf app/docs
RUN rm -Rf app/venv
RUN rm -Rf app/.git
RUN rm -Rf app/.github
RUN rm -Rf app/.gitignore

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]
