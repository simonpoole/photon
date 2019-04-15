package de.komoot.photon.elasticsearch;

import de.komoot.photon.PhotonDoc;

/**
 * Container class for an update action and a PhotonDoc
 * 
 * @author Simon
 *
 */
public class PhotonAction {

    public enum ACTION {CREATE, DELETE, DELETE_OSM, UPDATE, UPDATE_OR_CREATE};
    
    public ACTION action;
    
    public String id;
    
    public PhotonDoc doc;
    
    public String osmType;
    
    public long osmId;
    
    public String osmKey;
    
    public String osmValue;
}
