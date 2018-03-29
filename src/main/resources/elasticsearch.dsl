
============================================literature================================================================


PUT /front_literature_title_clicknum
PUT /front_literature_title_clicknum/_mapping/title_clicknum
{
  "properties": {
          "addTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
          },
          "numvalue": {
            "type": "double"
          },
          "name": {
            "type": "keyword"
          }
        }
}


============================================literature================================================================


============================================book======================================================================
PUT /front_book_index
PUT /front_book_index/_mapping/front_book_type
{
  "properties": {
          "addTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
          },
          "numvalue": {
            "type": "double"
          },
          "name": {
            "type": "keyword"
          },
          "category": {
                  "type": "keyword"
          }
        }
}
============================================book======================================================================