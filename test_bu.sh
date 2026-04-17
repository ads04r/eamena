HERITAGE_PLACE=34cfe98e-c2c0-11ea-9026-02e7594ce0a0
BASE_DIR="$(dirname "$(realpath "$0")")"
ENV_DIR="$(dirname $BASE_DIR)/ENV"
BU_DIR="$(dirname $BASE_DIR)/bulk_uploads"
IFS=$(echo -en "\n\r\b")

source $ENV_DIR/bin/activate

for f in $( find $BU_DIR | grep for_import | grep "xlsx.json$" )
do
	INPUT_FILE=$( echo "$f" | sed "s#/for_import/#/#" | sed "s/.json$//" )
	RES=$( python $BASE_DIR/manage.py bu -o validate -g $HERITAGE_PLACE -s "$INPUT_FILE" )

	if [ "$RES" != "[]" ]
	then
		echo "$INPUT_FILE"
		echo "$RES"
		echo ""
	fi

done

deactivate
