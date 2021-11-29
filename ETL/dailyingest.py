export PROJECT=googlerecommend434prod
gcloud config set project $PROJECT

gsutil mb -c regional -l us-central1 gs://$PROJECT

sudo pip install virtualenv

virtualenv -p python3 venv
source venv/bin/activate
pip install apache-beam[gcp]==2.24.0

python -m googlerecommend434prod/ETL/data_ingestion.py --project=$PROJECT --region=us-central1 --runner=DataflowRunner --staging_location=gs://$PROJECT/stage --temp_location gs://$PROJECT/test --input gs://$PROJECT/googlerecommend4344prod --save_main_session
