package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.util.I18nUtil;

/**
 * trigger type enum
 *
 * @author xuxueli 2018-09-16 04:56:41
 */
public enum TriggerTypeEnum {

    MANUAL(I18nUtil.getString("jobconf_trigger_type_manual")),
    /**
     * CORN 模式
     */
    CRON(I18nUtil.getString("jobconf_trigger_type_cron")),
    RETRY(I18nUtil.getString("jobconf_trigger_type_retry")),
    PARENT(I18nUtil.getString("jobconf_trigger_type_parent")),
    API(I18nUtil.getString("jobconf_trigger_type_api")),
    /**
     * 调度过期
     */
    MISFIRE(I18nUtil.getString("jobconf_trigger_type_misfire"));

    private TriggerTypeEnum(String title) {
        this.title = title;
    }

    private String title;

    public String getTitle() {
        return title;
    }

}
