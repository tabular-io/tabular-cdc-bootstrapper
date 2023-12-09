# Start with Python 3.9
FROM python:3.9

# Set work directory in the container
WORKDIR /app

# Install pipenv
RUN pip install pipenv

# Install dependencies using Pipfile
COPY ./tabular-cdc-bootstrapper/Pipfile* ./
RUN pipenv install --deploy --ignore-pipfile

# Copy just Python and JSON files
COPY ./tabular-cdc-bootstrapper/*.py ./tabular-cdc-bootstrapper/*.json ./

# Run batch_execute.py
CMD [ "pipenv", "run", "python", "batch_execute.py" ]