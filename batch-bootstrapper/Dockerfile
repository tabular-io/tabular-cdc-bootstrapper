# Start with Python 3.9
FROM python:3.9

# Set work directory in the container
WORKDIR /app

# Install pipenv
RUN pip install pipenv

# Install dependencies using Pipfile
COPY ./Pipfile* ./
RUN pipenv install

# Copy src files
COPY ./*.py ./


# Run batch bootstrapper
CMD [ "pipenv", "run", "python", "batch_bootstrapper.py" ]