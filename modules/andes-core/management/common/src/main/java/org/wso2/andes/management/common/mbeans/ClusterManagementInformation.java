package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import java.util.List;
import javax.management.JMException;
import javax.management.MBeanException;

/**
 * <code>ClusterManagementInformation</code>
 * Exposes the Cluster Management related information
 */
public interface ClusterManagementInformation {

    /**
     * MBean type name
     */
    static final String TYPE = "ClusterManagementInformation";

    /**
     * Checks if clustering is enabled
     *
     * @return whether clustering is enabled
     */
    @MBeanAttribute(name = "isClusteringEnabled", description = "is in clustering mode")
    boolean isClusteringEnabled();

    /**
     * Gets node ID assigned for the node
     *
     * @return node ID
     */
    @MBeanAttribute(name = "getMyNodeID", description = "Node Id assigned for the node")
    String getMyNodeID();

    /**
     * Gets all the address of the nodes in a cluster
     *
     * @return A list of address of the nodes in a cluster
     */
    @MBeanAttribute(name = "getAllClusterNodeAddresses", description = "Gets the addresses of the members in a cluster")
    List<String> getAllClusterNodeAddresses() throws JMException;

    /**
     * Gets the message store's health status
     *
     * @return true if healthy, else false.
     */
    @MBeanAttribute(name = "getStoreHealth", description = "Gets the message stores health status")
    boolean getStoreHealth();

    /**
     * Get the <Hostname>:<amqp_port> of node given queue is owned. This node handles all subscriptions
     * for the given queue. If queue is not assigned still, this call assigns a node for the queue and
     * return information.
     *
     * @param queueName name of queue
     * @param protocol protocol dealt with. Ports will be returned considering this
     * @return <Hostname>:<amqp_port> of assigned node
     * @throws MBeanException exception on internal error
     */
    String getOwningNodeOfQueue(
            @MBeanOperationParameter(name = "queueName" ,description ="get queue name ?") String queueName,
            @MBeanOperationParameter(name = "protocol" ,description =" protocol (amqp/mqtt) to get port?") String
                    protocol)
            throws MBeanException;
}
