package com.atguigu.member.bean

/**
  * 样例类拉链表 --> 子集
  *
  * @param uid
  * @param paymoney
  * @param vip_level
  * @param start_time
  * @param end_time
  * @param dn
  */
case class MemberZipper(
                         uid: Int,
                         var paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         dn: String
                       )

/**
  * 样例类拉链表
  *
  * @param list
  */
case class MemberZipperResult(list: List[MemberZipper])

/**
  * 指标样例类
  *
  * @param uid
  * @param ad_id
  * @param memberlevel
  * @param register
  * @param appregurl
  * @param regsource
  * @param regsourcename
  * @param adname
  * @param siteid
  * @param sitename
  * @param vip_level
  * @param paymoney
  * @param dt
  * @param dn
  */
case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        //注册来源url
                        appregurl: String,
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        dt: String,
                        dn: String
                      )

/**
  * dws用户宽表样例类
  *
  * @param uid
  * @param ad_id
  * @param fullname
  * @param iconurl
  * @param lastlogin
  * @param mailaddr
  * @param memberlevel
  * @param password
  * @param paymoney
  * @param phone
  * @param qq
  * @param register
  * @param regupdatetime
  * @param unitname
  * @param userip
  * @param zipcode
  * @param appkey
  * @param appregurl
  * @param bdp_uuid
  * @param reg_createtime
  * @param domain
  * @param isranreg
  * @param regsource
  * @param regsourcename
  * @param adname
  * @param siteid
  * @param sitename
  * @param siteurl
  * @param site_delete
  * @param site_createtime
  * @param site_creator
  * @param vip_id
  * @param vip_level
  * @param vip_start_time
  * @param vip_end_time
  * @param vip_last_modify_time
  * @param vip_max_free
  * @param vip_min_free
  * @param vip_next_level
  * @param vip_operator
  * @param dt
  * @param dn
  */
case class DwsMember(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: BigDecimal,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: String,
                      domain: String,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: String,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: String,
                      vip_level: String,
                      vip_start_time: String,
                      vip_end_time: String,
                      vip_last_modify_time: String,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String
                    )

/**
  * 用户宽表样例类 ---> result
  *
  * @param uid
  * @param ad_id
  * @param fullname
  * @param icounurl
  * @param lastlogin
  * @param mailaddr
  * @param memberlevel
  * @param password
  * @param paymoney
  * @param phone
  * @param qq
  * @param register
  * @param regupdatetime
  * @param unitname
  * @param userip
  * @param zipcode
  * @param appkey
  * @param appregurl
  * @param bdp_uuid
  * @param reg_createtime
  * @param domain
  * @param isranreg
  * @param regsource
  * @param regsourcename
  * @param adname
  * @param siteid
  * @param sitename
  * @param siteurl
  * @param site_delete
  * @param site_createtime
  * @param site_creator
  * @param vip_id
  * @param vip_level
  * @param vip_start_time
  * @param vip_end_time
  * @param vip_last_modify_time
  * @param vip_max_free
  * @param vip_min_free
  * @param vip_next_level
  * @param vip_operator
  * @param dt
  * @param dn
  */
case class DwsMember_Result(
                             uid: Int,
                             ad_id: Int,
                             fullname: String,
                             icounurl: String,
                             lastlogin: String,
                             mailaddr: String,
                             memberlevel: String,
                             password: String,
                             paymoney: String,
                             phone: String,
                             qq: String,
                             register: String,
                             regupdatetime: String,
                             unitname: String,
                             userip: String,
                             zipcode: String,
                             appkey: String,
                             appregurl: String,
                             bdp_uuid: String,
                             reg_createtime: String,
                             domain: String,
                             isranreg: String,
                             regsource: String,
                             regsourcename: String,
                             adname: String,
                             siteid: String,
                             sitename: String,
                             siteurl: String,
                             site_delete: String,
                             site_createtime: String,
                             site_creator: String,
                             vip_id: String,
                             vip_level: String,
                             vip_start_time: String,
                             vip_end_time: String,
                             vip_last_modify_time: String,
                             vip_max_free: String,
                             vip_min_free: String,
                             vip_next_level: String,
                             vip_operator: String,
                             dt: String,
                             dn: String
                           )