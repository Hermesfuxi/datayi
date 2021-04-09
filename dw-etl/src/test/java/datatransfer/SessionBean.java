package datatransfer;

import java.io.Serializable;

public class SessionBean implements Serializable {
    private String guid         ;
    private String session_id   ;
    private String start_time   ;
    private String end_time     ;
    private String start_page   ;
    private String end_page     ;
    private String pv_cnt       ;
    private String isnew        ;
    private String hour_range   ;
    private String country      ;
    private String province     ;
    private String city         ;
    private String region       ;
    private String device_type  ;
    private String os_name      ;
    private String release_ch   ;

    public void set(String guid, String session_id, String start_time, String end_time, String start_page, String end_page, String pv_cnt, String isnew, String hour_range, String country, String province, String city, String region, String device_type, String os_name, String release_ch) {
        this.guid = guid;
        this.session_id = session_id;
        this.start_time = start_time;
        this.end_time = end_time;
        this.start_page = start_page;
        this.end_page = end_page;
        this.pv_cnt = pv_cnt;
        this.isnew = isnew;
        this.hour_range = hour_range;
        this.country = country;
        this.province = province;
        this.city = city;
        this.region = region;
        this.device_type = device_type;
        this.os_name = os_name;
        this.release_ch = release_ch;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public String getStart_page() {
        return start_page;
    }

    public void setStart_page(String start_page) {
        this.start_page = start_page;
    }

    public String getEnd_page() {
        return end_page;
    }

    public void setEnd_page(String end_page) {
        this.end_page = end_page;
    }

    public String getPv_cnt() {
        return pv_cnt;
    }

    public void setPv_cnt(String pv_cnt) {
        this.pv_cnt = pv_cnt;
    }

    public String getIsnew() {
        return isnew;
    }

    public void setIsnew(String isnew) {
        this.isnew = isnew;
    }

    public String getHour_range() {
        return hour_range;
    }

    public void setHour_range(String hour_range) {
        this.hour_range = hour_range;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public String getOs_name() {
        return os_name;
    }

    public void setOs_name(String os_name) {
        this.os_name = os_name;
    }

    public String getRelease_ch() {
        return release_ch;
    }

    public void setRelease_ch(String release_ch) {
        this.release_ch = release_ch;
    }


    @Override
    public String toString() {
        return "SessionBean{" +
                "guid='" + guid + '\'' +
                ", session_id='" + session_id + '\'' +
                ", start_time='" + start_time + '\'' +
                ", end_time='" + end_time + '\'' +
                ", start_page='" + start_page + '\'' +
                ", end_page='" + end_page + '\'' +
                ", pv_cnt='" + pv_cnt + '\'' +
                ", isnew='" + isnew + '\'' +
                ", hour_range='" + hour_range + '\'' +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", region='" + region + '\'' +
                ", device_type='" + device_type + '\'' +
                ", os_name='" + os_name + '\'' +
                ", release_ch='" + release_ch + '\'' +
                '}';
    }
}
