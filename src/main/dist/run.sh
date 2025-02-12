. /etc/profile
APPNAME=clinical-trials-pipeline
APPDIR=/data/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=jthota@mcw.edu
if [ "$SERVER" = "MORN" ]; then
  EMAIL_LIST=jthota@mcw.edu,jdepons@mcw.edu
fi
cd $APPDIR
pwd
DB_OPTS="-Dspring.config=/data/pipelines/properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
export  CLINICAL_TRAILS_PIPELINE_OPTS="$DB_OPTS $LOG4J_OPTS"
bin/$APPNAME "$@" | tee run.log
mailx -s "[$SERVER] Gene Therapy Clinical trials Indexing Pipeline OK" $EMAIL_LIST < run.log