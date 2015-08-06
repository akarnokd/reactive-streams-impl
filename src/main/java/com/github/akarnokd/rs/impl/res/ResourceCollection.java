package com.github.akarnokd.rs.impl.res;

public interface ResourceCollection extends Resource {
    void add(Resource resource);
    
    void remove(Resource resource);
    
    /**
     * Removes but does not call close on the given resource.
     * @param resource
     */
    void delete(Resource resource);
    
    void clear();
}
