require('isomorphic-fetch');
require('isomorphic-form-data');

// const fetch = require('node-fetch');

const moment = require('moment');
const XLSX = require('xlsx');
const http = require('http');
const { deleteFeatures, addFeatures, queryFeatures } = require('@esri/arcgis-rest-feature-layer');
const restAuth = require('@esri/arcgis-rest-auth');

const liveFeatureServiceUrl = 'https://services.arcgis.com/LG9Yn2oFqZi5PnO5/arcgis/rest/services/Armed_Conflict_Location_Event_Data_ACLED/FeatureServer/0';

const translateToFeatureJson = data => {
  return data.map(event => {
    return {
      geometry: {
        x: parseFloat(event.longitude),
        y: parseFloat(event.latitude)
      },
      attributes: {
        data_id: parseInt(event.data_id),
        iso: event.iso,
        event_id_cnty: event.event_id_cnty,
        event_id_no_cnty: event.event_id_no_cnty,
        event_date: moment(event.event_date).format('YYYY-MM-DD'),
        year: parseInt(event.year),
        time_precision: event.time_precision,
        event_type: event.event_type,
        sub_event_type: event.sub_event_type,
        actor1: event.actor1,
        assoc_actor_1: event.assoc_actor_1,
        inter1: event.inter1,
        actor2: event.actor2,
        assoc_actor_2: event.assoc_actor_2,
        inter2: event.inter2,
        interaction: event.interaction,
        region: event.region,
        country: event.country,
        admin1: event.admin1,
        admin2: event.admin2,
        admin3: event.admin3,
        location: event.location,
        latitude: parseFloat(event.latitude),
        longitude: parseFloat(event.longitude),
        geo_precision: event.geo_precision,
        source: event.source,
        source_scale: event.source_scale,
        notes: event.notes,
        fatalities: parseInt(event.fatalities),
        timestamp: event.timestamp,
        iso3: event.iso3
      }
    };
  });
};

const translateUSAToFeatureJson = data => {
  return data.map(event => {
    return {
      geometry: {
        x: parseFloat(event.LONGITUDE),
        y: parseFloat(event.LATITUDE)
      },
      attributes: {
        iso: event.ISO,
        event_id_cnty: event.EVENT_ID_CNTY,
        event_id_no_cnty: event.EVENT_ID_NO_CNTY,
        event_date: moment(event.EVENT_DATE).format('YYYY-MM-DD'),
        year: parseInt(event.YEAR),
        time_precision: event.TIME_PRECISION,
        event_type: event.EVENT_TYPE,
        sub_event_type: event.SUB_EVENT_TYPE,
        actor1: event.ACTOR1,
        assoc_actor_1: event.ASSOC_ACTOR_1,
        inter1: event.INTER1,
        actor2: event.ACTOR2,
        assoc_actor_2: event.ASSOC_ACTOR_2,
        inter2: event.INTER2,
        interaction: event.INTERACTION,
        region: event.REGION,
        country: event.COUNTRY,
        admin1: event.ADMIN1,
        admin2: event.ADMIN2,
        admin3: event.ADMIN3,
        location: event.LOCATION,
        latitude: parseFloat(event.LATITUDE),
        longitude: parseFloat(event.LONGITUDE),
        geo_precision: event.GEO_PRECISION,
        source: event.SOURCE,
        source_scale: event.SOURCE_SCALE,
        notes: event.NOTES,
        fatalities: parseInt(event.FATALITIES),
        iso3: 'USA'
      }
    };
  });
};

const deleteLiveFeatures = async session => {
  const deleteParams = {
    url: liveFeatureServiceUrl,
    params: { where: '1=1' },
    authentication: session
  };
  return deleteFeatures(deleteParams);
};

const deleteLiveFeaturesByIds = async (oids, session) => {
  const deleteParams = {
    url: liveFeatureServiceUrl,
    objectIds: oids,
    authentication: session
  };
  return deleteFeatures(deleteParams);
};

const queryFeaturesForIds = async session => {
  const params = {
    url: liveFeatureServiceUrl,
    where: '1=1',
    returnIdsOnly: true,
    authentication: session
  };
  return queryFeatures(params).then(response => response.objectIds);
}

const insertLiveFeatures = (newData, session) => {
  console.log(`inserting ${newData.length} features ..`);
  const addParams = {
    url: liveFeatureServiceUrl,
    features: newData,
    authentication: session
  };
  return addFeatures(addParams);
};

const initAuth = async () => {
  return new Promise((resolve, reject) => {
    let session = null;
    try {
      session = new restAuth.UserSession({
        username: process.env.SERVICE_USER,
        password: process.env.SERVICE_PASS
      });
      resolve(session);
    } catch (error) {
      reject(new Error('unable to get authentication setup'));  
    }
  });
};

const getLiveAcledData = async () => {
  const fourteenDaysAgo = moment().subtract(14, 'days').format('YYYY-MM-DD');
  const apiUrl = `https://api.acleddata.com/acled/read?event_date=${fourteenDaysAgo}&event_date_where=%3E=&limit=0&terms=accept&key=${process.env.ACLED_KEY}&email=${process.env.ACLED_USER}`

  // context.log('requesting data from ACLED API ..');
  // context.log(`ACLED API request URL :: ${apiUrl}`);

  return fetch(apiUrl)
    .then(response => response.json())
    .then(responseData => {
      if (!responseData) {
        throw new Error('no response data returned from ACLED API');
      } else if (responseData.count === 0 || responseData.data.length === 0) {
        return [];
      } else {
        return translateToFeatureJson(responseData.data);        
      }
    });
};

const getUSData = async () => {
    const res = await fetch(`http://acleddata.com/download/22846/`);
    const buffer = await res.buffer();
    const workbook = XLSX.read(buffer, {type: "buffer", cellDates: true, dateNF: 'yy-mm-dd'});
    const sheetName = Object.keys(workbook.Sheets)[0]
    const worksheet = workbook.Sheets[sheetName];
    const json = XLSX.utils.sheet_to_json(worksheet); 
    let features = translateUSAToFeatureJson(json);

    const fourteenDaysAgo = moment().subtract(14, 'days');
    features = features.filter(feature => {
      const featDate = moment(feature.attributes.event_date);
      if (featDate >= fourteenDaysAgo) {
        return true;
      }
      return false
    });
    
    return features;
    
}

const chunk = (arr, size) => {
  return Array.from({ 
    length: Math.ceil(arr.length / size) 
  }, 
  (v, i) => arr.slice(i * size, i * size + size));
}

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if (myTimer.IsPastDue) {
        context.log('JavaScript is running late!');
    }

    context.log('ACLED Update Initiated', timeStamp);     

    const sessionInfo = await initAuth();

    // update ACLED global data
    let newData = null;
    try {
      newData = await getLiveAcledData();      
    } catch (error) {
      context.log(error); 
    }

    // update USA data
    let usaNewData = null;
    try {
      usaNewData = await getUSData();   
      // context.log(usaNewData)   
    } catch (error) {
      context.log(error); 
    }

    if (usaNewData.length === 0 && newData.length === 0) {
      context.log('ACLED Update Completed. No data returned for both ACLED Global and ACLED USA'); 
      return context.done();
    }

    // combine both acled global and acled usa arrays
    const featuresToAdd = [...newData, ...usaNewData];
    
    let deleteResponse = null;
    try {
      const oids = await queryFeaturesForIds(sessionInfo);
      const oidChunks = chunk(oids, 500);
      let deletedFeatures = [];
      for (let i=0; i < oidChunks.length; i++) {
        context.log(`deleting chunk ${i+1} of ${oidChunks.length} ...`);
        deleteResponse = await deleteLiveFeaturesByIds(oidChunks[i], sessionInfo);
        deletedFeatures.push(deleteResponse.deleteResults);
      }
      context.log(`deleted ${deletedFeatures.length} features`); 
    } catch (error) {
      context.log(error); 
    }

    let addResponse = null;
    let results = 0;
    try {
      const chunks = chunk(featuresToAdd, 500);
      for (let i=0; i < chunks.length;i++) {
        addResponse = await insertLiveFeatures(chunks[i], sessionInfo);
        results += addResponse.addResults.length;
      }
    } catch (error) {
      context.log(error); 
    }

    context.log(`successfully added ${results} features`);
    context.log('ACLED Live update completed');
};