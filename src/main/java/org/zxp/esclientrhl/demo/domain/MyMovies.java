package org.zxp.esclientrhl.demo.domain;

import org.zxp.esclientrhl.annotation.ESID;
import org.zxp.esclientrhl.annotation.ESMapping;
import org.zxp.esclientrhl.annotation.ESMetaData;
import org.zxp.esclientrhl.enums.DataType;

import java.util.List;

/**
 * @program: esdemo
 * @description:
 * @author: X-Pacific zhang
 * @create: 2020-01-01 11:22
 **/
@ESMetaData(indexName = "my_movies", number_of_shards = 1,number_of_replicas = 0,printLog = true)
public class MyMovies {
    @ESID
    private String id;
    @ESMapping
    private String title;

    @ESMapping(datatype = DataType.nested_type,nested_class = Actors.class)
    private List<Actors> actors;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Actors> getActors() {
        return actors;
    }

    public void setActors(List<Actors> actors) {
        this.actors = actors;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "MyMovies{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", actors=" + actors +
                '}';
    }
}
