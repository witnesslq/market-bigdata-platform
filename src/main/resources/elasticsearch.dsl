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