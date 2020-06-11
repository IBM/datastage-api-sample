/*
 * Copyright 2020 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <windows.h>
#include <stdio.h>
#include <string.h>
#include <malloc.h>
#include <ctype.h>
#include <dsapi.h>

#ifndef BOOL
#define BOOL int
#endif

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

#define DSJE_DSJOB_ERROR        -9999

/*****************************************************************************/
/*
 * Print out the given string list one string per line, prefixing each
 * string by the specified number of tabs.
 */
static void printStrList(
    int indent,                 /* Number of tabs to indent */
    char *str                   /* String list to print */
)
{
    int i;
    while(*str != '\0')
    {
        for (i = 0; i < indent; i++)
            putchar('\t');
        printf("%s\n", str);
        str += (strlen(str) + 1);
    }
}

/*****************************************************************************/
/*
 * Print out the details of a job log entry. Each line we print is prefixed
 * by the requested number of tabs.
 */
static void printLogDetail(
    int indent,                 /* Number of tabs to indent by */
    DSLOGDETAIL *logDetail      /* The log entry to print */
)
{
    char prefix[6] = "\t\t\t\t\t";
    prefix[indent] = '\0';
    printf("%sEvent Id: ", prefix);
    if (logDetail->eventId < 0)
        printf("unknown");
    else
        printf("%d", logDetail->eventId);
    printf("\n");
    printf("%sTime\t: %s", prefix, ctime(&(logDetail->timestamp)));
    printf("%sType\t: ", prefix);
    switch(logDetail->type)
    {
    case DSJ_LOGINFO:
        printf("INFO");
        break;
    case DSJ_LOGWARNING:
        printf("WARNING");
        break;
    case DSJ_LOGFATAL:
        printf("FATAL");
        break;
    case DSJ_LOGREJECT:
        printf("REJECT");
        break;
    case DSJ_LOGSTARTED:
        printf("STARTED");
        break;
    case DSJ_LOGRESET:
        printf("RESET");
        break;
    case DSJ_LOGBATCH:
        printf("BATCH");
        break;
    case DSJ_LOGOTHER:
        printf("OTHER");
        break;
    default:
        printf("????");
        break; 
    }
    printf("\n");
    printf("%sMessage\t:\n", prefix);
    printStrList(indent+1, logDetail->fullMessage);
}

/*****************************************************************************/
#define MAX_PARAMS 10 /* Arbitrary value */
/*
 * Set a jobs parameter at the server end based on the "name=value" string
 * that we are given. This involves asking the server what the type of
 * the parameter is, converting the value part to that type and constructing
 * a DSPARAM value, and then calling DSSetParam to register the parameter and
 * its value.
 */
static int setParam(
    DSJOB hJob,             /* Job parameter belongs to */
    char *param             /* param=value string */
)
{
    char *value;
    int status;
    DSPARAMINFO paramInfo;
    DSPARAM paramData;
    /* Get the parameter name and its value string */
    value = strchr(param, '=');
    *value++ = '\0';
    /* Get the parameter information which tells us what type it is */
    status = DSGetParamInfo(hJob, param, &paramInfo);
    if (status != DSJE_NOERROR)
        fprintf(stderr, "Error %d getting information for parameter '%s'\n", status, param);
    else
    {
        /*
         * Construct the value structure to pass to the server. We could
         * attempt to validate some of these parameters rather than
         * simply copying them... but for simplicity we don't!
         */
        paramData.paramType = paramInfo.paramType;
        switch(paramInfo.paramType)
        {
        case DSJ_PARAMTYPE_STRING:
            paramData.paramValue.pString = value;
            break;
        case DSJ_PARAMTYPE_ENCRYPTED:
            paramData.paramValue.pEncrypt = value;
            break;
        case DSJ_PARAMTYPE_INTEGER:
            paramData.paramValue.pInt = atoi(value);
            break;
        case DSJ_PARAMTYPE_FLOAT:
            paramData.paramValue.pFloat = (float) atof(value);
            break;
        case DSJ_PARAMTYPE_PATHNAME:
            paramData.paramValue.pPath = value;
            break;
        case DSJ_PARAMTYPE_LIST:
            paramData.paramValue.pListValue = value;
            break;
        case DSJ_PARAMTYPE_DATE:
            paramData.paramValue.pDate = value;
            break;
        case DSJ_PARAMTYPE_TIME:
            paramData.paramValue.pTime = value;
            break;
        default:        /* try string!!!! */
            paramData.paramType = DSJ_PARAMTYPE_STRING;
            paramData.paramValue.pString = value;
            break;
        }
        /* Try setting the parameter */
        status = DSSetParam(hJob, param, &paramData);
        if (status != DSJE_NOERROR)
            fprintf(stderr, "Error setting value of parameter '%s'\n", param);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -run sub-command
 */
static int jobRun(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    int i;
    char *project;
    char *job;
    int mode = DSJ_RUNNORMAL;
    int warningLimit = -1;
    int rowLimit = 0;
    BOOL badOptions = FALSE;
    char *param[MAX_PARAMS];
    int nParams = 0;
    BOOL waitForJob = FALSE;
    /* Validate arguments and extract optional arguments */
    for (i = 0; (i < argc) && !badOptions && (
                (argv[i][0] == '-') || (argv[i][0] == '/')); i++)
    {
        char *opt = &(argv[i][1]);
        if (strcmp(opt, "wait") == 0)
            waitForJob = TRUE;
        else
        {
            char *arg = argv[i+1];
            if (++i >= argc)
                badOptions = TRUE;
            else if (strcmp(opt, "mode") == 0)
            {
                if (strcmp(arg, "NORMAL") == 0)
                    mode = DSJ_RUNNORMAL;
                else if (strcmp(arg, "RESET") == 0)
                    mode = DSJ_RUNRESET;
                else if (strcmp(arg, "VALIDATE") == 0)
                    mode = DSJ_RUNVALIDATE;
                else
                    badOptions = TRUE;
            }
            else if (strcmp(opt, "param") == 0)
            {
                if (strchr(arg, '=') == NULL)
                    badOptions = TRUE;
                else
                    param[nParams++] = arg;
            }
            else if (strcmp(opt, "warn") == 0)
                warningLimit = atoi(arg);
            else if (strcmp(opt, "rows") == 0)
                rowLimit = atoi(arg);
            else
                badOptions = TRUE;
        }
    }
    /* Must be two parameters left... project and job */
    if ((i+2) == argc)
    {
        project = argv[i];
        job = argv[i+1];
    }
    else
        badOptions = TRUE;
    /* Report validation problems and exit */
    if (badOptions)
    {
        fprintf(stderr, "Invalid arguments: dsjob -run\n");
        fprintf(stderr, "\t\t\t[-mode <NORMAL | RESET | VALIDATE>]\n");
        fprintf(stderr, "\t\t\t[-param <name>=<value>]\n");
        fprintf(stderr, "\t\t\t[-warn <n>]\n");
        fprintf(stderr, "\t\t\t[-rows <n>]\n");
        fprintf(stderr, "\t\t\t[-wait]\n");
        fprintf(stderr, "\t\t\t<project> <job>\n");
        return DSJE_DSJOB_ERROR;
    }
    /* Attempt to open the project, open the job and lock it */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            if ((status = DSLockJob(hJob)) != DSJE_NOERROR)
                fprintf(stderr, "ERROR: Failed to lock job\n");
            else
            {
                /* Now set any job attributes and try running the job */
                if (warningLimit >= 0)
                {
                    status = DSSetJobLimit(hJob, DSJ_LIMITWARN, warningLimit);
                    if (status != DSJE_NOERROR)
                        fprintf(stderr, "Error setting warning limit\n");
                }
                if ((rowLimit != 0) && (status == DSJE_NOERROR))
                {
                    status = DSSetJobLimit(hJob, DSJ_LIMITROWS, rowLimit);
                    if (status != DSJE_NOERROR)
                        fprintf(stderr, "Error setting row limit\n");
                }
                for (i = 0; (status == DSJE_NOERROR) && (i < nParams); i++)
                    status = setParam(hJob, param[i]);
                if (status == DSJE_NOERROR)
                {
                    status = DSRunJob(hJob, mode);
                    if (status != DSJE_NOERROR)
                        fprintf(stderr, "Error running job\n");
                }
                /* Now wait for the job to finish */
                if ((status == DSJE_NOERROR) && waitForJob)
                {
                    printf("Waiting for job...\n");
                    status = DSWaitForJob(hJob);
                    if (status != DSJE_NOERROR)
                        fprintf(stderr, "Error waiting for job\n");
                }
                (void) DSUnlockJob(hJob);
            }
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -stop sub-command
 */
static int jobStop(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    /* Validate arguments and extract optional arguments */
    /* Must be two parameters left... project and job */
    if (argc != 2)
    {
        fprintf(stderr, "Invalid arguments: dsjob -stop <project> <job>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Now stop the job */
            status = DSStopJob(hJob);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error stopping job\n");
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -lprojects sub-command
 */
static int jobLProjects(int argc, char *argv[])
{
    int result = DSJE_NOERROR;
    char *list;
    /* Validate arguments */
    if (argc != 0)
    {
        fprintf(stderr, "Invalid arguments: dsjob -lproject\n");
        return DSJE_DSJOB_ERROR;
    }
    /* Action request */
    list = DSGetProjectList();
    if (list == NULL)
        result = DSGetLastError();
    else
        printStrList(0, list);
    return result;
}

/*****************************************************************************/
/*
 * Handle the -ljobs sub-command
 */
static int jobLJobs(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSPROJECTINFO pInfo;
    int status;
    /* Validate arguments */
    if (argc != 1)
    {
        fprintf(stderr, "Invalid arguments: dsjob -ljobs <project>\n");
        return DSJE_DSJOB_ERROR;
    }
    /* Action request */
    hProject = DSOpenProject(argv[0]);
    if (hProject == NULL)
        status = DSGetLastError();
    else
    {
        status = DSGetProjectInfo(hProject, DSJ_JOBLIST, &pInfo);
        if (status == DSJE_NOT_AVAILABLE)
        {
            printf("<none>\n");
            status = DSJE_NOERROR;
        }
        else if (status == DSJE_NOERROR)
            printStrList(0, pInfo.info.jobList);
        DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -lstages sub-command
 */
static int jobLStages(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    DSJOBINFO jobInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be two parameters left... project and job */
    if (argc != 2)
    {
        fprintf(stderr, "Invalid arguments: dsjob -lstages <project> <job>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
		{
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Get the list of stages */
            status = DSGetJobInfo(hJob, DSJ_STAGELIST, &jobInfo);
            if (status == DSJE_NOT_AVAILABLE)
            {
                printf("<none>\n");
                status = DSJE_NOERROR;
            }
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting stage list\n", status);
            else
                printStrList(0, jobInfo.info.stageList);
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -llinks sub-command
 */
static int jobLLinks(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    char *stage;
    DSSTAGEINFO stageInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be three parameters left... project, job and stage */
    if (argc != 3)
    {
        fprintf(stderr, "Invalid arguments: dsjob -llinks <project> <job> <stage>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    stage = argv[2];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
		{
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Get the list of stages */
            status = DSGetStageInfo(hJob, stage, DSJ_LINKLIST, &stageInfo);
            if (status == DSJE_NOT_AVAILABLE)
            {
                printf("<none>\n");
                status = DSJE_NOERROR;
            }
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting link list\n", status);
            else
                printStrList(0, stageInfo.info.linkList);
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -jobinfo sub-command
 */
static int jobJobInfo(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    DSJOBINFO jobInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be two parameters left... project and job */
    if (argc != 2)
    {
        fprintf(stderr, "Invalid arguments: dsjob -jobinfo <project> <job>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
	{
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /*
             * Try getting all the job info (except the stage and
             * parameter lists which we deal with elsewhere)
             */
            status = DSGetJobInfo(hJob, DSJ_JOBSTATUS, &jobInfo);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting job status\n", status);
            else
            {
                printf("Job Status\t: ");
                switch(jobInfo.info.jobStatus)
                {
                case DSJS_RUNNING:
                    printf("RUNNING");
                    break;
                case DSJS_RUNOK:
                    printf("RUN OK");
                    break;
                case DSJS_RUNWARN:
                    printf("RUN with WARNINGS");
                    break;
                case DSJS_RUNFAILED:
                    printf("RUN FAILED");
                    break;
                case DSJS_VALOK:
                    printf("VALIDATED OK");
                    break;
                case DSJS_VALWARN:
                    printf("VALIDATE with WARNINGS");
                    break;
                case DSJS_VALFAILED:
                    printf("VALIDATION FILED");
                    break;
                case DSJS_RESET:
                    printf("RESET");
                    break;
                case DSJS_STOPPED:
                    printf("STOPPED");
                    break;
                case DSJS_NOTRUNNABLE:
                    printf("NOT COMPILED");
                    break;
                case DSJS_NOTRUNNING:
                    printf("NOT RUNNING");
                    break;
                default:
                    printf("UNKNOWN");
                    break;
                }
                printf(" (%d)\n", jobInfo.info.jobStatus);
            }
            status = DSGetJobInfo(hJob, DSJ_JOBCONTROLLER, &jobInfo);
            if (status == DSJE_NOT_AVAILABLE)
                printf("Job Controller\t: not available\n");
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting job controller\n", status);
            else
                printf("Job Controller\t: %s\n", jobInfo.info.jobController);
            status = DSGetJobInfo(hJob, DSJ_JOBSTARTTIMESTAMP, &jobInfo);
            if (status == DSJE_NOT_AVAILABLE)
                printf("Job Start Time\t: not available\n");
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting job start time\n", status);
            else
                printf("Job Start Time\t: %s", ctime(&(jobInfo.info.jobStartTime)));
            status = DSGetJobInfo(hJob, DSJ_JOBWAVENO, &jobInfo);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting job wave number\n", status);
            else
                printf("Job Wave Number\t: %d\n", jobInfo.info.jobWaveNumber);
            status = DSGetJobInfo(hJob, DSJ_USERSTATUS, &jobInfo);
            if (status == DSJE_NOT_AVAILABLE)
                printf("User Status\t: not available\n");
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting job user status\n", status);
            else
                printf("User Status\t: %s\n", jobInfo.info.userStatus);
            if (status == DSJE_NOT_AVAILABLE)
                status = DSJE_NOERROR;
            (void) DSCloseJob(hJob);    
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -stageinfo sub-command
 */
static int jobStageInfo(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    char *stage;
    DSSTAGEINFO stageInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be three parameters left... project, job and stage */
    if (argc != 3)
    {
        fprintf(stderr, "Invalid arguments: dsjob -stageinfo <project> <job> <stage>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    stage = argv[2];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /*
             * Try getting all the stage info (except the link
             * lists which we deal with elsewhere)
             */
            status = DSGetStageInfo(hJob, stage, DSJ_STAGETYPE, &stageInfo);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting stage type\n", status);
            else
                printf("Stage Type\t: %s\n", stageInfo.info.typeName);
            status = DSGetStageInfo(hJob, stage, DSJ_STAGEINROWNUM, &stageInfo);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting stage row number\n", status);
            else
                printf("In Row Number\t: %d\n", stageInfo.info.inRowNum);
            status = DSGetStageInfo(hJob, stage, DSJ_STAGELASTERR, &stageInfo);
            if (status == DSJE_NOT_AVAILABLE)
                printf("Stage Last Error: <none>\n");
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting stage last error\n", status);
            else
            {
                printf("Stage Last Error:\n");
                printLogDetail(1, &(stageInfo.info.lastError));
            }
            if (status == DSJE_NOT_AVAILABLE)
                status = DSJE_NOERROR;
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -linkinfo sub-command
 */
static int jobLinkInfo(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    char *stage;
    char *link;
    DSLINKINFO linkInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be four parameters left... project, job, stage and link names */
    if (argc != 4)
    {
        fprintf(stderr, "Invalid arguments: dsjob -linkinfo <project> <job> <stage> <link>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    stage = argv[2];
    link = argv[3];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Try getting all the link info */
            status = DSGetLinkInfo(hJob, stage, link, DSJ_LINKROWCOUNT, &linkInfo);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting link row count\n", status);
            else
                printf("Link Row Count\t: %d\n", linkInfo.info.rowCount);
            status = DSGetLinkInfo(hJob, stage, link, DSJ_LINKLASTERR, &linkInfo);
            if (status == DSJE_NOT_AVAILABLE)
            {
                printf("Link Last Error\t: <none>\n");
                status = DSJE_NOERROR;
            }
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting link last error\n", status);
            else
            {
                printf("Link Last Error\t:\n");
                printLogDetail(1, &(linkInfo.info.lastError));
            }
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -lparams sub-command
 */
static int jobLParams(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    DSJOBINFO jobInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be two parameters left... project and job names */
    if (argc != 2)
    {
        fprintf(stderr, "Invalid arguments: dsjob -lparams <project> <job>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Get the list of parameter names */
            status = DSGetJobInfo(hJob, DSJ_PARAMLIST, &jobInfo);
            if (status == DSJE_NOT_AVAILABLE)
            {
                printf("<none>\n");
                status = DSJE_NOERROR;
            }
            else if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting parameter list\n", status);
            else
                printStrList(0, jobInfo.info.paramList);
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -paraminfo sub-command
 */
static void printValue(
    DSPARAM *param
)
{
    switch(param->paramType)
    {
    case DSJ_PARAMTYPE_STRING:
        printf("%s", param->paramValue.pString);
        break;
    case DSJ_PARAMTYPE_ENCRYPTED:
        printf("%s", param->paramValue.pEncrypt);
        break;
    case DSJ_PARAMTYPE_INTEGER:
        printf("%d", param->paramValue.pInt);
        break;
    case DSJ_PARAMTYPE_FLOAT:
        printf("%G", param->paramValue.pFloat);
        break;
    case DSJ_PARAMTYPE_PATHNAME:
        printf("%s", param->paramValue.pPath);
        break;
    case DSJ_PARAMTYPE_LIST:
        printf("%s", param->paramValue.pListValue);
        break;
    case DSJ_PARAMTYPE_DATE:
        printf("%s", param->paramValue.pDate);
        break;
    case DSJ_PARAMTYPE_TIME:
        printf("%s", param->paramValue.pTime);
        break;
    }
}

static int jobParamInfo(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    char *param;
    DSPARAMINFO paramInfo;
    /* Validate arguments and extract optional arguments */
    /* Must be three parameters left... project, job and parameter names */
    if (argc != 3)
    {
        fprintf(stderr, "Invalid arguments: dsjob -paraminfo <project> <job> <param>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    param = argv[2];
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Get the parameter information */
            status = DSGetParamInfo(hJob, param, &paramInfo);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting info for parameter\n", status);
            else
            {
                printf("Type\t\t: ");
                switch(paramInfo.paramType)
                {
                case DSJ_PARAMTYPE_STRING:
                    printf("String");
                    break;
                case DSJ_PARAMTYPE_ENCRYPTED:
                    printf("Encrypted");
                    break;
                case DSJ_PARAMTYPE_INTEGER:
                    printf("Integer");
                    break;
                case DSJ_PARAMTYPE_FLOAT:
                    printf("Float");
                    break;
                case DSJ_PARAMTYPE_PATHNAME:
                    printf("Pathname");
                    break;
                case DSJ_PARAMTYPE_LIST:
                    printf("list");
                    break;
                case DSJ_PARAMTYPE_DATE:
                    printf("Date");
                    break;
                case DSJ_PARAMTYPE_TIME:
                    printf("Time");
                    break;
                default:
                    printf("*** ERROR - UNKNOWN TYPE ***");
                    break;
                }
                printf(" (%d)\n", paramInfo.paramType);

                printf("Help Text\t: %s\n", paramInfo.helpText);
                printf("Prompt\t\t: %s\n", paramInfo.paramPrompt);
                printf("Prompt At Run\t: %d\n", paramInfo.promptAtRun);
                printf("Default Value\t: ");
                printValue(&(paramInfo.defaultValue));
                printf("\n");
                printf("Original Default: ");
                printValue(&(paramInfo.desDefaultValue));
                printf("\n");
                if (paramInfo.paramType == DSJ_PARAMTYPE_LIST)
                {
                    printf("List Values\t:\n");
                    printStrList(2, paramInfo.listValues);
                    printf("Original List\t:\n");
                    printStrList(2, paramInfo.desListValues);
                }
                printf("\n");
            }
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -log sub-command
 */
static int jobLog(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    int i;
    char *project;
    char *job;
    int type = DSJ_LOGINFO;
    BOOL badOptions = FALSE;
    /* Validate arguments and extract optional arguments */
    for (i = 0; (i < argc) && !badOptions && (
                (argv[i][0] == '-') || (argv[i][0] == '/')); i++)
    {
        char *opt = &(argv[i][1]);
        /* Note: not mutually exclusive check on info or warn */
        if (strcmp(opt, "info") == 0)
            type = DSJ_LOGINFO;
        else if (strcmp(opt, "warn") == 0)
            type = DSJ_LOGWARNING;
        else
            badOptions = TRUE;
    }
    /* Must be two parameters left... project and job */
    if ((i+2) == argc)
    {
        project = argv[i];
        job = argv[i+1];
    }
    else
        badOptions = TRUE;
    /* Report validation problems and exit */
    if (badOptions)
    {
        fprintf(stderr, "Invalid arguments: dsjob -log\n");
        fprintf(stderr, "\t\t\t[-info | -warn]\n");
        fprintf(stderr, "\t\t\t<project> <job>\n");
        fprintf(stderr, "\nLog message is read from stdin.\n");
        return DSJE_DSJOB_ERROR;
    }
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
		if ((hJob = DSOpenJob(hProject, job)) == NULL)
		{
			status = DSGetLastError();
			fprintf(stderr, "ERROR: Failed to open job\n");
		}
		else
		{
			#define MAX_MSG_LEN 4096
			char message[MAX_MSG_LEN + 4];
			int n = 0;
			/* Read the message from stdin */
			printf("Enter message text, terminating with Ctrl-d\n");
			while (n < MAX_MSG_LEN)
			{
				int ch;
				ch = getchar();
				if ((ch == EOF)
#ifdef WIN32
								|| (ch == 4)    /* Ctrl-d */
#endif
						)
					break;
				if ((ch == '\n') || isprint(ch))
					message[n++] = ch;
			}
			printf("\nMessage read.\n");
			message[n] = '\0';
			/* Add message to the log */
			status = DSLogEvent(hJob, type, NULL, message);
			if (status != DSJE_NOERROR)
				fprintf(stderr, "Error adding log entry\n");
			(void) DSCloseJob(hJob);
		}
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -logsum sub-command
 */
static int jobLogSum(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    int i;
    char *project;
    char *job;
    int type = DSJ_LOGANY;
    time_t startTime = 0;
    time_t endTime = 0;
    int maxNumber = 0;
    BOOL badOptions = FALSE;
    /* Validate arguments and extract optional arguments */
    for (i = 0; (i < argc) && !badOptions && (
                (argv[i][0] == '-') || (argv[i][0] == '/')); i++)
    {
        char *opt = &(argv[i][1]);
        char *arg = argv[i+1];
        if (++i >= argc)
            badOptions = TRUE;
        else if (strcmp(opt, "type") == 0)
        {
            if (strcmp(arg, "INFO") == 0)
                type = DSJ_LOGINFO;
            else if (strcmp(arg, "WARNING") == 0)
                type = DSJ_LOGWARNING;
            else if (strcmp(arg, "FATAL") == 0)
                type = DSJ_LOGFATAL;
            else if (strcmp(arg, "REJECT") == 0)
                type = DSJ_LOGREJECT;
            else if (strcmp(arg, "STARTED") == 0)
                type = DSJ_LOGSTARTED;
            else if (strcmp(arg, "RESET") == 0)
                type = DSJ_LOGRESET;
            else if (strcmp(arg, "BATCH") == 0)
                type = DSJ_LOGBATCH;
            else if (strcmp(arg, "OTHER") == 0)
                type = DSJ_LOGOTHER;
            else
                badOptions = TRUE;
        }
        else if (strcmp(opt, "max") == 0)
            maxNumber = atoi(arg);
        else
                badOptions = TRUE;
    }
    /* Must be two parameters left... project and job */
    if ((i+2) == argc)
    {
        project = argv[i];
        job = argv[i+1];
    }
    else
        badOptions = TRUE;
    /* Report validation problems and exit */
    if (badOptions)
    {
        fprintf(stderr, "Invalid arguments: dsjob -logsum\n");
        fprintf(stderr, "\t\t\t[-type <INFO | WARNING | FATAL | REJECT | STARTED | RESET | BATCH>]\n");
        fprintf(stderr, "\t\t\t[-max <n>]\n");
        fprintf(stderr, "\t\t\t<project> <job>\n");
        return DSJE_DSJOB_ERROR;
    }
    /* Attempt to open the project, open the job and lock it */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            DSLOGEVENT event;
            /* Make the first call to establish the log info */
            status = DSFindFirstLogEntry(hJob, type, startTime,
                                endTime, maxNumber, &event);
            while(status == DSJE_NOERROR)
            {
                printf("%d\t", event.eventId);
                switch(event.type)
                {
                case DSJ_LOGINFO:
                    printf("INFO");
                    break;
                case DSJ_LOGWARNING:
                    printf("WARNING");
                    break;
                case DSJ_LOGFATAL:
                    printf("FATAL");
                    break;
                case DSJ_LOGREJECT:
                    printf("REJECT");
                    break;
                case DSJ_LOGSTARTED:
                    printf("STARTED");
                    break;
                case DSJ_LOGRESET:
                    printf("RESET");
                    break;
                case DSJ_LOGBATCH:
                    printf("BATCH");
                    break;
                case DSJ_LOGOTHER:
                    printf("OTHER");
                        break;
                default:
                    printf("????");
                    break;
                }
                printf("\t%s", ctime(&(event.timestamp))); /* ctime has \n at end */
                printf("\t%s\n", event.message);
                /* Go on to next entry */
                status = DSFindNextLogEntry(hJob, &event);
            }
            if (status == DSJE_NOMORE)
                status = DSJE_NOERROR;
            else
                fprintf(stderr, "Error %d getting log summary\n", status);
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -logdetail sub-command
 */
static int jobLogDetail(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status;
    char *project;
    char *job;
    int eventId;
    DSLOGDETAIL logDetail;
    /* Validate arguments and extract optional arguments */
    /* Must be three parameters left... project, job and event id */
    if (argc != 3)
    {
        fprintf(stderr, "Invalid arguments: dsjob -logdetail <project> <job> <event id>\n");
        return DSJE_DSJOB_ERROR;
    }
    project = argv[0];
    job = argv[1];
    eventId = atoi(argv[2]);
    /* Attempt to open the project and the job */
    if ((hProject = DSOpenProject(project)) == NULL)
    {
        status = DSGetLastError();
        fprintf(stderr, "ERROR: Failed to open project\n");
    }
    else
    {
        if ((hJob = DSOpenJob(hProject, job)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open job\n");
        }
        else
        {
            /* Try getting all the log info */
            status = DSGetLogEntry(hJob, eventId, &logDetail);
            if (status != DSJE_NOERROR)
                fprintf(stderr, "Error %d getting event details\n", status);
            else
                printLogDetail(0, &logDetail);
            (void) DSCloseJob(hJob);
        }
        (void) DSCloseProject(hProject);
    }
    return status;
}

/*****************************************************************************/
/*
 * Handle the -lognewest sub-command
 */
static int jobLogNewest(int argc, char *argv[])
{
    DSPROJECT hProject;
    DSJOB hJob;
    int status = DSJE_NOERROR;
    char *project;
    char *job;
    int type = DSJ_LOGANY;
    BOOL badOptions = FALSE;
    int id;
    /* Validate arguments and extract optional arguments */
    /* Must be at least two parameters left... project, job. Type is optional */
    if ((argc < 2) || (argc > 3))
            badOptions = TRUE;
        else
        {
            project = argv[0];
            job = argv[1];
            if (argc == 3)
            {
                char *arg = argv[2];
                if (strcmp(arg, "INFO") == 0)
                    type = DSJ_LOGINFO;
                else if (strcmp(arg, "WARNING") == 0)
                    type = DSJ_LOGWARNING;
                else if (strcmp(arg, "FATAL") == 0)
                    type = DSJ_LOGFATAL;
                else if (strcmp(arg, "REJECT") == 0)
                    type = DSJ_LOGREJECT;
                else if (strcmp(arg, "STARTED") == 0)
                    type = DSJ_LOGSTARTED;
                else if (strcmp(arg, "RESET") == 0)
                    type = DSJ_LOGRESET;
                else if (strcmp(arg, "BATCH") == 0)
                    type = DSJ_LOGBATCH;
                else if (strcmp(arg, "OTHER") == 0)
                    type = DSJ_LOGOTHER;
                else
                    badOptions = TRUE;
            }
        }
        if (badOptions)
        {
            fprintf(stderr, "Invalid arguments: dsjob -lognewest <project> <job> [<event type>]\n");
            fprintf(stderr, "\t event type = INFO | WARNING | FATAL | REJECT | STARTED | RESET | BATCH\n");
            return DSJE_DSJOB_ERROR;
        }
        /* Attempt to open the project and the job */
        if ((hProject = DSOpenProject(project)) == NULL)
        {
            status = DSGetLastError();
            fprintf(stderr, "ERROR: Failed to open project\n");
        }
        else
        {
            if ((hJob = DSOpenJob(hProject, job)) == NULL)
            {
                status = DSGetLastError();
                fprintf(stderr, "ERROR: Failed to open job\n");
            }
            else
            {
                /* Get the newest it */
                id = DSGetNewestLogId(hJob, type);
                if (id < 0)
                {
                    status = DSGetLastError();
                    fprintf(stderr, "Error %d getting event details\n", status);
                }
                else
                    printf("Newsest id = %d\n", id);
                    (void) DSCloseJob(hJob);
                }
            (void) DSCloseProject(hProject);
        }
    return status;
}

/*****************************************************************************/
/*
 * The following array defines all the allowed/known primary command options
 * and the routines that are used to process them.
 */
static struct MAJOROPTION
{
    char *name;
    int (*optionHandler) (int, char **);
} MajorOption[] =
{
    "run",              jobRun,
    "stop",             jobStop,
    "lprojects",        jobLProjects,
    "ljobs",            jobLJobs,
    "lstages",          jobLStages,
    "llinks",           jobLLinks,
    "jobinfo",          jobJobInfo,
    "stageinfo",        jobStageInfo,
    "linkinfo",         jobLinkInfo,
    "lparams",          jobLParams,
    "paraminfo",        jobParamInfo,
    "log",              jobLog,
    "logsum",           jobLogSum,
    "logdetail",        jobLogDetail,
    "lognewest",        jobLogNewest
};
#define N_MAJOR_OPTIONS (sizeof(MajorOption) / sizeof(struct MAJOROPTION))
/*
 * Main routine... simple!
 *
 * See if we have one of the optional server/user/password arguments. Then
 * Check that we have a primary command option and call the handler for that
 * option
 */
int main(
    int argc,                   /* Argument count */
    char *argv[]                /* Argument strings */
)
{
    int i;
    int argPos;
	char *domain = NULL;
    char *server = NULL;
    char *user = NULL;
    char *password = NULL;
    int result = DSJE_NOERROR;

    /* Must have at least one argument */
    if (argc < 2)
        goto reportError;
    argc--; /* Remove command name */

    /* Check for the optional paramaters... not that they are positional */
    argPos = 1;
	/* Domain[:<port>] name */
	/* For equivalent of domain NONE do not specify this argument */
    if (strcmp(argv[argPos], "-domain") == 0)
    {
        if (argc < 3)
            goto reportError;
        domain = argv[argPos + 1];
		argPos += 2;
        argc -= 2;
    }
    /* Server name */
    if (strcmp(argv[argPos], "-server") == 0)
    {
        if (argc < 3)
            goto reportError;
        server = argv[argPos + 1];
		argPos += 2;
        argc -= 2;
    }
    /* User name */
    if (strcmp(argv[argPos], "-user") == 0)
	{
        if (argc < 3)
            goto reportError;
        user = argv[argPos + 1];
		argPos += 2;
        argc -= 2;
    }
    /* Password */
    if (strcmp(argv[argPos], "-password") == 0)
	{
        if (argc < 3)
            goto reportError;
        password = argv[argPos + 1];
		argPos += 2;
        argc -= 2;
    }

    /* Must be at least one command argument remaining... */
    if (argc < 1)
        goto reportError;
    /* ... that must start with a '-' (or '/' on NT)... */
    if ((argv[argPos][0] != '-')
#ifdef WIN32
                && (argv[argPos][0] != '/')
#endif
                )
        goto reportError;

    /* ... and it must be one of the primary commands... */
    for (i = 0; i < N_MAJOR_OPTIONS; i++)
	{
        if (strcmp(&(argv[argPos][1]), MajorOption[i].name) == 0)
        {
            char *errText;

            DSSetServerParams(domain, user, password, server);

            result = MajorOption[i].optionHandler(argc - 1, &(argv[argPos + 1]));

            if (result != DSJE_NOERROR)
                fprintf(stderr, "\nStatus code = %d\n", result);

            errText = DSGetLastErrorMsg(NULL);
            if (errText != NULL)
            {
                fflush(stdout);
                fprintf(stderr, "\nLast recorded error message =\n");
                printStrList(0, errText);
                fflush(stdout);
                fprintf(stderr, "\n");
            }
            goto exitProgram;
        }
	}

    /* We only get here if we failed to find a valid command */
    fprintf(stderr, "Invalid/unknown primary command switch.\n");
reportError:
    fprintf(stderr, "Command syntax:\n");
    fprintf(stderr, "\tdsjob [-domain <domain>][-server <server>][-user <user>][-password <password>]\n");
    fprintf(stderr, "\t\t\t<primary command> [<arguments>]\n");
    fprintf(stderr, "\nValid primary command options are:\n");
    for (i = 0; i < N_MAJOR_OPTIONS; i++)
        fprintf(stderr, "\t-%s\n", MajorOption[i].name);
    result = DSJE_DSJOB_ERROR;

exitProgram:
    return result;
}

/* End of module */
