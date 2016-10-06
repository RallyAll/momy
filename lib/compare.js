'use strict';
//TODO write tests for this

const _isCollectionChanged = ({collection, table}) => {
  const newAttr = collection.list.filter( field => {
    return !table.list.some( tfield => field.field === tfield.field );
  });

  const deletedAttr = table.list.filter( tfield => {
    return !collection.list.some( field => field.field === tfield.field );
  });

  if(deletedAttr.length || newAttr.length){
    //Added these for debuging - 
    collection.newAttr = newAttr;
    collection.deletedAttr = deletedAttr;
    return true;
  }
  return false;
};

const objectToList = object => {
  return Object.keys(object).map( key => {
    const fields = Object.keys(object[key]).map( field => {
      return {
        field,
        type: object[key][field]
      };
    });
    return {
      name: key,
      list: fields
    };
  });
};

const listToObject = list => {
  const object = {};
  list.forEach( item => {
    const fields = {};
    item.list.forEach( field => {
      fields[field.field] = field.type;
    });
    object[item.name] = fields;
  });
  return object;
};

//Finds collections in mongo that
//aren't in mysql
const findNewCollections = ({mysql, mongo}) => {
  return mongo.filter( collection => {
    return !mysql.some( table => table.name === collection.name );
  });
};

const findUpdatedCollections = ({mysql, mongo}) => {
  return mongo.filter( collection => {
    return mysql.some( table => table.name === collection.name );
  }).filter( collection => {
    const table = mysql.find( table => table.name === collection.name );
    return _isCollectionChanged({table, collection});
  });
};

const findUnchangedCollections = ({mysql, mongo}) => {
  return mongo.filter( collection => {
    return mysql.some( table => table.name === collection.name );
  }).filter( collection => {
    const table = mysql.find( table => table.name === collection.name );
    return !_isCollectionChanged({table, collection});
  });
};

module.exports = {
  objectToList,
  listToObject,
  findNewCollections,
  findUpdatedCollections,
  findUnchangedCollections
};
