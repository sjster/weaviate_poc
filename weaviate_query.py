import weaviate
import json
import yaml
import requests
import secrets

class WeaviateSS:
    def __init__(self):
        with open("secrets", 'r') as stream:
            data_loaded = yaml.safe_load(stream)

        self.client = weaviate.Client(
            url = data_loaded['WEAVIATE_URL'],  # Replace with your endpoint
            auth_client_secret=weaviate.AuthApiKey(api_key=data_loaded['WEAVIATE_API_KEY']),  # Replace w/ your Weaviate instance API key
            additional_headers = {
                "X-HuggingFace-Api-Key": data_loaded['HUGGINGFACE_API_KEY']  # Replace with your inference API key
            }
        )

    def create_batch_import_cashflow(self) -> None:
        with open("sample_cashflow.json", 'r') as stream:
            data_loaded = yaml.safe_load(stream)

        # Configure a batch process
        with self.client.batch(
            batch_size=100
        ) as batch:
            # Batch import all Questions
            for ans_q_pair in zip(data_loaded['questions'], data_loaded['answers']):
                print(ans_q_pair)

                properties = {
                    "question": ans_q_pair[0],
                    "answer": ans_q_pair[1],
                    "category": "Cashflow",
                }

                self.client.batch.add_data_object(
                    properties,
                    "Question",
                )

    def create_class_questions(self) -> None:
        class_obj = {
            "class": "Cashflow",
            "vectorizer": "text2vec-huggingface",  # If set to "none" you must always provide vectors yourself. Could be any other "text2vec-*" also.
            "moduleConfig": {
                "text2vec-huggingface": {
                    "model": "sentence-transformers/multi-qa-mpnet-base-dot-v1",  # Can be any public or private Hugging Face model.
                    "options": {
                        "waitForModel": True
                    }
                }
            }
        }

        self.client.schema.create_class(class_obj)

    def query_vectordb(self, query) -> str:
        nearText = {"concepts": query}

        response = (
            self.client.query
            .get("Question", ["question", "answer", "category"])
            .with_near_text(nearText)
            .with_limit(2)
            .do()
        )

        return(json.dumps(response, indent=4))

weaviate_obj = WeaviateSS()
#weaviate_obj.create_class_questions()
#weaviate_obj.create_batch_import_cashflow()
res = weaviate_obj.query_vectordb(query="My cashflow has been low the last couple of months, how do I resolve it?")
print(res)
