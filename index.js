'use strict'
const ba = require('ymuto-dynamodb-client');
const bam = require('ymuto-s3-client');

/** @class */
module.exports = class ReadOperation {
  constructor () {
    this.params = {
      MAX_BATCH_GET : 100
    }

    this.aliasVersion = "";
  }

  prepareScanMasParams ( params ) {
    return ba.scn(params);
  }

  prepareScanMasParamsAll ( params ) {
    return ba.scnAll(params);
  }


  prepareScanMas(filename) {
    let param = {
      file_path : filename
    };
//    return bam.scn(param);
    return bam.scnAlias(param, this.aliasVersion);    
  }

  qry ( params ) 
  {
    return ba.qry(params);
  }

  makeUserQuery ( user_id, table_name, proj_attrs ) {
    let params = {};
    params.TableName                 = table_name;
    params.KeyConditionExpression    = '#r = :r',
    params.ExpressionAttributeNames  = {
      "#r": "user_id"
    };
    params.ExpressionAttributeValues = {
      ":r": user_id
    };
    params.ProjectionExpression      = this._makeProjStringOption(proj_attrs);
    return ba.qry(params);
  }

  _makeProjStringOption ( a ) {
    if ( !a ) { return; }
    let projExpress = "";
    let max = a.length;
    a.forEach( ( v, i ) => {
      if ( i < max - 1 ){
        projExpress += v + ", ";
      } else {
        projExpress += v;
      }
    });
    return projExpress;
  }

  makeGet ( key, table_name, proj_attrs ) {
    let tn = {};
    tn = {
      TableName : table_name,
      Key : key,
      ProjectionExpression : this._makeProjStringOption(proj_attrs)
    };
    return ba.get(tn);
  }


  makeBtgs ( keys, table_name, proj_attrs ) {
    let length = Math.ceil(keys.length/this.params.MAX_BATCH_GET);
    let req_s = [];

    for ( let i = 0; i < length - 1; i++ )
    {
      let eachKeys = [];
      for ( let j = 0; j < this.params.MAX_BATCH_GET; j++ )
      {
        eachKeys.push(keys[j+i*this.params.MAX_BATCH_GET]);
      }
      req_s.push(this._makeRequest(table_name, eachKeys, proj_attrs));
    }

    let eachKeys = [];
    for ( let j = (length-1)*this.params.MAX_BATCH_GET; j < keys.length ; j++ )
    {
      eachKeys.push(keys[j]);
    }
    req_s.push(this._makeRequest(table_name, eachKeys, proj_attrs));

//    console.log("ReadOperation::makeBtgs " + JSON.stringify(req_s));
    return ba.btgs(req_s);
  }

  _makeRequest ( table_name, keys, proj_attrs ) {
    let tn = {};
    tn[table_name] = {
      Keys : keys,
      ProjectionExpression : this._makeProjStringOption(proj_attrs)
    };
    return {
      RequestItems: tn
    };
  }

  // 要素数が 100 を超える場合には対応していない。
  makeBtgMultiTableHashsRangesProjection ( x ) {
    let ht_s  = x.hash_table_name_s;
    let hhk_s = x.hash_hash_key_name_s;
    let hh_s  = x.hash_hash_keys_s;
    let hp_s  = x.hash_proj_attrs_s;


    let rt_s  = x.range_table_name_s;
    let rhk_s = x.range_hash_key_name_s;
    let rrk_s = x.range_range_key_name_s;
    let rh_s  = x.range_hash_key_s_s;
    let rs_s  = x.range_range_keys_s_s;
    let rp_s  = x.range_proj_attrs_s;

    let requestItems = {};
    if ( ht_s ) {
      ht_s.forEach ( (t, i) => {
        let hk = hhk_s[i];
        let h  = hh_s[i];
        let ps = hp_s[i];

        if ( h.length == 0 ) {
          return;
        }

        Object.assign(requestItems,this._makeBtgRequestItemHashDirect(t, hk, h, ps));
      });
    } 

    if ( rt_s ) {
      rt_s.forEach ( (t, i) => {
        let hk  = rhk_s[i];
        let rk  = rrk_s[i];
        let h  = rh_s[i];
        let rs = rs_s[i];
        let ps = rp_s[i];

        if ( h.length == 0 ){
          return;
        };
        let isRsNotExist = rs.some((v,i)=>{
          return v.length == 0;
        });
        if ( isRsNotExist )
        {
          return;
        }

        Object.assign(requestItems,this._makeBtgRequestItemRangeSsDirect(t, hk, rk, h, rs, ps));
      });
    }
    let request =  {
      RequestItems: requestItems
    };
    return ba.btg(request);
  }


  _makeBtgRequestItemRangeSsDirect ( t, hk, rk, hs, rs, ps ) {
    let keys = [];
    hs.forEach((h, i)=>{
        keys = keys.concat(this._makeBtgRequestItemKeysRangeDirect(hk, rk, h, rs[i]));
    });
    let ri = {};
    ri[t] = this._makeKeyItemsDirect(keys, ps); 
    return ri;
  }

  _makeBtgRequestItemHashDirect ( t, hk, hs, ps ){
    let keys = this._makeBtgRequestItemKeysHashDirect(hk, hs);
    let ri = {}
    ri[t] = this._makeKeyItemsDirect(keys, ps); 
    return ri;
  }

  _makeBtgRequestItemKeysHashDirect (hk, hs){
    let keys = [];
    hs.forEach( ( h, i ) => {
      let key = {};
      key[hk] = h;
      keys.push(key);
    });
    return keys;
  }

  _makeKeyItemsDirect ( k, ps ) {
    if (!ps ){
      return {
        Keys: k
      };
    } else {
      return {
        Keys: k,
        ProjectionExpression: this._makeProjectionString(ps)
      };
    }
  }

  _makeProjectionString ( a ) {
    let projExpress = "";
    let max = a.length;
    a.forEach( ( v, i ) => {
      if ( i < max - 1 ){
        projExpress += v + ", ";
      } else {
        projExpress += v;
      }
    });
    return projExpress;
  }

  _makeBtgRequestItemKeysRangeDirect (hk, rk, h, rs ) {
    let keys = [];
    rs.forEach( ( r, i ) => {
      let key = {};
      key[hk] = h;
      key[rk] = r;
      keys.push(key);
    });
    return keys;
  } 

};