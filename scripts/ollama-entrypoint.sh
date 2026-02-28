#!/bin/bash
set -e

echo "Starting Ollama server..."
ollama serve &
OLLAMA_PID=$!

echo "Waiting for Ollama to be ready..."
until ollama list > /dev/null 2>&1; do
    sleep 2
    echo "Ollama not ready yet, retrying..."
done

echo "Ollama is ready. Checking models..."

FILTER_MODEL="${OLLAMA_FILTER_MODEL:-phi3:mini}"
ANALYSIS_MODEL="${OLLAMA_ANALYSIS_MODEL:-mistral:7b}"

pull_if_missing() {
    local model="$1"
    if ! ollama list | grep -q "$model"; then
        echo "Pulling model: $model"
        ollama pull "$model"
        echo "Model $model pulled successfully."
    else
        echo "Model $model already present."
    fi
}

pull_if_missing "$FILTER_MODEL"
pull_if_missing "$ANALYSIS_MODEL"

echo "All models ready. Ollama running with PID $OLLAMA_PID."
wait $OLLAMA_PID
