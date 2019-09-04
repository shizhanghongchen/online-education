#!/bin/bash

dt=$2
dn=$3
echo $dt

import_ads_register_adnamenum(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_adnamenum.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_register_appregurlnum(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_appregurlnum.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_register_memberlevelnum(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_memberlevelnum.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_register_regsourcenamenum(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_regsourcenamenum.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_register_sitenamenum(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_sitenamenum.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_register_top3memberpay(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_top3memberpay.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_register_viplevelnum(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_register_viplevelnum.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_low3_userdetail(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_low3_userdetail.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_paper_avgtimeandscore(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_paper_avgtimeandscore.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_paper_maxdetail(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_paper_maxdetail.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_paper_scoresegment_user(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_paper_scoresegment_user.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_top3_userdetail(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_top3_userdetail.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_user_paper_detail(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_user_paper_detail.json -p "-Ddt=${dt} -Ddn=${dn}"
}

import_ads_user_question_detail(){
  python /opt/module/datax/bin/datax.py /opt/module/datax/job/ads_user_question_detail.json -p "-Ddt=${dt} -Ddn=${dn}"
}

case $1 in
  "ads_register_adnamenum")
     import_ads_register_adnamenum
;;
  "ads_register_appregurlnum")
     import_ads_register_appregurlnum
;;
  "ads_register_memberlevelnum")
     import_ads_register_memberlevelnum
;;
  "ads_register_regsourcenamenum")
     import_ads_register_regsourcenamenum
;;
  "ads_register_sitenamenum")
     import_ads_register_sitenamenum
;;
  "ads_register_top3memberpay")
     import_ads_register_top3memberpay
;;
  "ads_register_viplevelnum")
     import_ads_register_viplevelnum
;;
  "ads_low3_userdetail")
     import_ads_low3_userdetail
;;
  "ads_paper_avgtimeandscore")
     import_ads_paper_avgtimeandscore
;;
  "ads_paper_maxdetail")
     import_ads_paper_maxdetail
;;
  "ads_paper_scoresegment_user")
     import_ads_paper_scoresegment_user
;;
  "ads_top3_userdetail")
     import_ads_top3_userdetail
;;
  "ads_user_paper_detail")
     import_ads_user_paper_detail
;;
  "ads_user_question_detail")
     import_ads_user_question_detail
;;
   "all")
   import_ads_register_adnamenum
   import_ads_register_appregurlnum
   import_ads_register_memberlevelnum
   import_ads_register_regsourcenamenum
   import_ads_register_sitenamenum
   import_ads_register_top3memberpay
   import_ads_register_viplevelnum
   import_ads_low3_userdetail
   import_ads_paper_avgtimeandscore
   import_ads_paper_maxdetail
   import_ads_paper_scoresegment_user
   import_ads_top3_userdetail
   import_ads_user_paper_detail
   import_ads_user_question_detail
;;
esac