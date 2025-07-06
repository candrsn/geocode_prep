
drop_index() {

    return 

    if [ -n "$1" ]; then
        index=$1
        curl -XDELETE "http://localhost:9200/${index}/"

        curl "http://localhost:9200/_cat/indices?v"
    fi

}

drop_type() {

}

drop_alias() {
    return

    index=$1
    alias=$2
    curl -XDELETE "http://localhost:9200/${name}/_alias/${alias}"
    
}

