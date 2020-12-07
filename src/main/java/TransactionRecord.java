import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * TransactionRecord contains a collection of audit objects that reflect all of the changes resulting from
 * a single transaction.
 *
 * @author Chris Upkes
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransactionRecord {
    private List<Audit> auditList = new ArrayList<>();

    public void addAudit (Audit audit) {
        this.auditList.add(audit);
    }

    public void setAuditList(List<Audit> auditList) {
        this.auditList = auditList;
    }

}
