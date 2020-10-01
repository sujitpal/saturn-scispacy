CORD19_RELEASES_URL=https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases/
DATE_YYYY_MM_DD=2020-08-28

TARBALL_NAME=cord-19_$DATE_YYYY_MM_DD.tar.gz
wget $CORD19_RELEASES_URL/$TARBALL_NAME
tar xvzf $TARBALL_NAME

cd $DATE_YYYY_MM_DD
tar xvzf document_parses.tar.gz 

cd ..
aws s3api put-object --bucket $BUCKET_NAME --key $DATE_YYYY_MM_DD
aws s3 cp $DATE_YYYY_MM_DD s3://$BUCKET_NAME/$DATE_YYYY_MM_DD --recursive

rm cord_19_embeddings.tar.gz document_parses.tar.gz changelog

