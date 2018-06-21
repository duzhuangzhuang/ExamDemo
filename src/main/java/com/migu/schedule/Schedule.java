package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/*
 *类名和方法不能修改
 */
public class Schedule
{
    // 注册节点列表
    private static List<Integer> nodeIdList = new ArrayList<Integer>(10);
    
    // 挂起列表 key=taskid ,value=consumption
    private static Map<Integer, Integer> hangUpList =
        new HashMap<Integer, Integer>(8);
    
    // 所有任务列表，包括运行中和挂起的 ，key=taskid ,value=consumption
    private static Map<Integer, Integer> allList =
        new HashMap<Integer, Integer>(8);
    
    // 运行中的任务列表
    private static List<TaskInfo> runTaskList = new ArrayList<TaskInfo>(10);
    
    /**
     * 初始化
     * 
     * @return
     */
    public int init()
    {
        nodeIdList.clear();
        hangUpList.clear();
        allList.clear();
        runTaskList.clear();
        return ReturnCodeKeys.E001;
    }
    
    /**
     * 注册节点
     * 
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId)
    {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        if (nodeIdList.contains(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        nodeIdList.add(nodeId);
        return ReturnCodeKeys.E003;
    }
    
    /**
     * 节点注销
     * 
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId)
    {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        if (!nodeIdList.contains(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        nodeIdList.remove(nodeId);
        return ReturnCodeKeys.E006;
    }
    
    /**
     * 5、 添加任务
     * 
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        if (hangUpList.keySet().contains(taskId))
        {
            return ReturnCodeKeys.E010;
        }
        hangUpList.put(taskId, consumption);
        allList.put(taskId, consumption);
        return ReturnCodeKeys.E008;
    }
    
    /**
     * 6、 删除任务
     * 
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        if (!allList.keySet().contains(taskId))
        {
            return ReturnCodeKeys.E012;
        }
        TaskInfo removetaskInfo = null;
        if (hangUpList.keySet().contains(taskId))
        {
            hangUpList.remove(taskId);
        }
        else
        {
            for (TaskInfo taskInfo : runTaskList)
            {
                if (taskId == taskInfo.getTaskId())
                {
                    removetaskInfo = taskInfo;
                }
            }
            if (null != removetaskInfo)
            {
                runTaskList.remove(removetaskInfo);
            }
        }
        allList.remove(taskId);
        return ReturnCodeKeys.E011;
    }
    
    /**
     * 7、 任务调度
     * 
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold)
    {
        if (threshold <= 0)
        {
            return ReturnCodeKeys.E002;
        }
        runTaskList.clear();
        // 消耗率分组
        ArrayList<List<Integer>> list = new ArrayList<List<Integer>>();
        // 所有的消耗率列表
        List<Integer> consumptions = new ArrayList<Integer>(allList.values());
        // 节点数
        int noidNum = nodeIdList.size();
        Collections.sort(consumptions);
        // 消耗率总和
        int consumptionSum = sumList(consumptions);
        // 商
        int quotient = consumptionSum / noidNum;
        // 余数
        int remainder = consumptionSum % noidNum;
        
        while (list.size() < noidNum - 1)
        {
            // 从最大值开始比较
            if (quotient <= consumptions.get(consumptions.size()))
            {
                List<Integer> list1 = new ArrayList<Integer>();
                list1.add(consumptions.get(consumptions.size()));
                list.add(list1);
                consumptions.remove(consumptions.size());
            }
            else
            {
                for (int i = 0; i < consumptions.size() - 1; i++)
                {
                    int num =
                        consumptions.get(consumptions.size())
                            + consumptions.get(i);
                    if ((num >= quotient) && (num - quotient) <= remainder)
                    {
                        List<Integer> list1 = new ArrayList<Integer>();
                        list1.add(consumptions.get(i));
                        list1.add(consumptions.get(consumptions.size()));
                        list.add(list1);
                        consumptions.remove(consumptions.size());
                        consumptions.remove(i);
                        break;
                    }
                }
            }
        }
        
        return ReturnCodeKeys.E000;
    }
    
    /**
     * 8、 查询当前任务状态
     * 
     * @param tasks
     * @return
     */
    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        if (null == tasks)
        {
            return ReturnCodeKeys.E016;
        }
        tasks.clear();
        tasks.addAll(runTaskList);
        Iterator<Integer> it = hangUpList.keySet().iterator();
        while (it.hasNext())
        {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setNodeId(-1);
            taskInfo.setTaskId(it.next());
            tasks.add(taskInfo);
        }
        Collections.sort(tasks, new Comparator<TaskInfo>()
        {
            
            public int compare(TaskInfo o1, TaskInfo o2)
            {
                // 按照taskid进行升序排列
                if (o1.getTaskId() > o2.getTaskId())
                {
                    return 1;
                }
                if (o1.getTaskId() == o2.getTaskId())
                {
                    return 0;
                }
                return -1;
            }
        });
        return ReturnCodeKeys.E015;
    }
    
    private int sumList(List<Integer> list)
    {
        int sum = 0;
        for (int i = 0; i < list.size(); i++)
        {
            sum = sum + list.get(i);
        }
        return sum;
        
    }
}
