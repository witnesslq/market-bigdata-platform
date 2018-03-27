============================================film===================================================================
PUT /front_film_playnum
PUT /front_film_playnum/_mapping/film_playnum
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

PUT /front_film_label_pie
PUT /front_film_label_pie/_mapping/label_pie
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

PUT /front_film_tit1e_score
PUT /front_film_tit1e_score/_mapping/tit1e_score
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


PUT /front_film_tit1e_company
PUT /front_film_tit1e_company/_mapping/tit1e_company
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

PUT /front_film_actor_playnum
PUT /front_film_actor_playnum/_mapping/actor_playnum
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


PUT /front_film_actor_score
PUT /front_film_actor_score/_mapping/actor_score
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

PUT /front_film_director_playnum
PUT /front_film_director_playnum/_mapping/director_playnum
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


PUT /front_film_director_score
PUT /front_film_director_score/_mapping/director_score
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
============================================film===================================================================

============================================soap===================================================================
PUT /front_soap_playnum
PUT /front_soap_playnum/_mapping/soap_playnum
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


PUT /front_soap_label_pie
PUT /front_soap_label_pie/_mapping/soap_label_pie
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



PUT /front_soap_score_title
PUT /front_soap_score_title/_mapping/soap_score_title
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


PUT /front_soap_guest
PUT /front_soap_guest/_mapping/soap_guest
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



PUT /front_soap_guest_comment
PUT /front_soap_guest_comment/_mapping/guest_comment
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
============================================soap===================================================================


============================================variety===================================================================
PUT /front_variety_playnum
PUT /front_variety_playnum/_mapping/variety_playnum
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



PUT /front_variety_label_pie
PUT /front_variety_label_pie/_mapping/variety_label_pie
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




PUT /front_variety_guest_playnum
PUT /front_variety_guest_playnum/_mapping/variety_guest_playnum
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

============================================variety===================================================================


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