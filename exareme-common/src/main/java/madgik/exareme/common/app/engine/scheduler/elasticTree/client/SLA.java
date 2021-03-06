package madgik.exareme.common.app.engine.scheduler.elasticTree.client;

import java.io.Serializable;

/**
 * @author Konstantinos Tsakalozos <br>
 *         University of Athens / Department of Informatics and Telecommunications.
 * @since 1.0
 */
public interface SLA extends Serializable {

    int getId();

    double getBudget(double execTime);
}
