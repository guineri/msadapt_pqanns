//
// Created by gnandrade on 24/03/2021.
//

#include "controller/StreamController.h"
#include <math.h>
#define PI 3.14159265

void StreamController::configure(Worker *w)
{
    this->w = w;
    auto *qp = dynamic_cast<QueryProcessor *>(w);
    this->maxThreads = w->config.MAXCPUCORES - w->config.DEDICATEDCORES;
    
    insertOuterLevel = 5;
    queryOuterLevel = 5;
    queryInnerLevel = 0;

    this->setupNewConfiguration(qLevels.at(queryOuterLevel).at(queryInnerLevel), iLevels.at(insertOuterLevel));
    qp->streamControllerStop = false;
    qp->updatedCheckStart = false;

    // Initial Stream Profile
    // Dynamic Stream
    double granularity = 0.5;
    double from = 0.1;
    double to = 0.5;
    double picos = 1.0;
    double changeAmount = ceil(qp->config.STREAM_TIME / granularity);
    double changeInterval = (qp->config.STREAM_TIME / changeAmount);
    double period = 60.0;

    qp->queryStreamRate.clear();
    qp->queryStreamChangeInterval = qp->config.STREAM_TIME;
    qp->queryStreamRate.push_back(qp->config.LOAD_FACTOR_QUERY);
    /*qp->queryStreamChangeInterval = changeInterval;
    for (int i = 0; i <= changeAmount; i++)
    {
        double value = from + (0.5 * (to - from))*(1 + sin(((PI*i)/ (period / picos)  - PI/2.0)));
        qp->queryStreamRate.push_back(value);
    }*/

    qp->insertStreamRate.clear();
    qp->insertStreamChangeInterval = qp->config.STREAM_TIME;
    qp->insertStreamRate.push_back(qp->config.LOAD_FACTOR_INSERT);
    /*to = 0.3;
    qp->insertStreamChangeInterval = changeInterval;
    for (int i = 0; i <= changeAmount; i++)
    {
        double value = to + (0.5 * (from - to))*(1 + sin(((PI*i)/ (period / picos)  - PI/2.0)));
        qp->insertStreamRate.push_back(value);
    }*/

}

bool StreamController::decrementInsertionResourcesLevels(int level)
{
    if (this->insertOuterLevel - level < 0) {
        return false;
    }

    this->insertOuterLevel -= level;
    this->queryOuterLevel += level;
    this->queryInnerLevel = 0;

    this->insertOuterThreads = iLevels.at(this->insertOuterLevel);
    this->queryOuterThreads = qLevels.at(this->queryOuterLevel).at(this->queryInnerLevel);
    return true;
}

bool StreamController::incrementInsertionResourcesLevels(int level)
{
    // Max
    if (this->insertOuterLevel + level > iLevels.size() - 1) {
        return false;
    }

    this->insertOuterLevel += level;
    this->queryOuterLevel -= level;
    this->queryInnerLevel = qLevels.at(this->queryOuterLevel).size() - 1;
    
    this->insertOuterThreads = iLevels.at(this->insertOuterLevel);
    this->queryOuterThreads = qLevels.at(this->queryOuterLevel).at(this->queryInnerLevel);
    return true;
}

bool StreamController::decrementQueryResourcesLevels(int level) 
{
    if (this->queryOuterLevel - level < 0) {
        return false;
    }

    if (this->queryInnerLevel == 0) {
        this->queryOuterLevel -= level;
        this->queryInnerLevel = qLevels.at(this->queryOuterLevel).size() - 1;
        this->insertOuterLevel += level;
    } else {
        this->queryInnerLevel -= 1;
    }

    this->insertOuterThreads = iLevels.at(this->insertOuterLevel);
    this->queryOuterThreads = qLevels.at(this->queryOuterLevel).at(this->queryInnerLevel);
    return true;
}

bool StreamController::incrementQueryResourcesLevels(int level) 
{
    if (this->queryOuterLevel + level > qLevels.size() - 1
        && this->queryInnerLevel == qLevels.at(this->queryOuterLevel).size() -1) {
        return false;
    }

    if (this->queryInnerLevel == qLevels.at(this->queryOuterLevel).size() - 1) {
        this->queryOuterLevel += level;
        this->queryInnerLevel = 0;
        this->insertOuterLevel -= level;
    } else {
        this->queryInnerLevel += 1;
    }
    
    this->insertOuterThreads = iLevels.at(this->insertOuterLevel);
    this->queryOuterThreads = qLevels.at(this->queryOuterLevel).at(this->queryInnerLevel);
    return true;
}

void StreamController::msAdaptLevel()
{
    auto *qp = dynamic_cast<QueryProcessor *>(w);
    if (qp->config.MSTEST) { //AQUI!!
        qp->wait_all->count_down_and_wait(); //AQUI!!
    } //AQUI!!

    // Parameters
    double  samplingInterval            = 0.5; //s
    double  currentTime                 = Time::getCurrentTime();
    double  currentTimeTemporalUpdate   = Time::getCurrentTime();
    bool    fineGrainQueryIncrease      = false;
    bool    fineGrainQueryDecreate      = false;
    bool    borCorrection               = false;
    bool    lazyInsertioActived         = false;
    bool    forcedInsertionIncrease     = false;
    bool    earlyInsertionIncrease      = false;
    double  deltaBOM                    = 0.0;
    bool    zeroBOF                     = false;
    // Initializing
    SCSignals signals;
    signals.setup(&qp->monitor);

    // BOR
    double t = qp->temporalInsertionsBuffer.getFlexibility();
    double borUpperBound = t * 0.5;
    double borLowerBound = borUpperBound * 0.75;

    if (t <= 0) {
        zeroBOF = true;
    }

    qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
    qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
    qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);
    qp->monitor.set(QPMonitor::Metric::CONF, this->queryOuterThreads + (0.1 * this->insertOuterThreads));

    while(!qp->queryProcessorCanFinalize && !qp->insertProcessorCanFinalize) {
        qp->monitor.add(QPMonitor::Metric::QQS, qp->queriesToProcessBufferList.size());
        qp->monitor.add(QPMonitor::Metric::BOR, qp->temporalInsertionsBuffer.getOutdatedRatio());
        qp->monitor.add(QPMonitor::Metric::BOM, qp->temporalInsertionsBuffer.getOutdatedMax());
        qp->monitor.add(QPMonitor::Metric::IQS, qp->temporalInsertionsBuffer.getSize());

        qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
        qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
        qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);

        if (Time::getCurrentTime() - currentTime > samplingInterval) {
            samplingInterval = 0.05;
            if (!qp->monitor.getCurrent(QPMonitor::Metric::QAR) == 0.0
                && !qp->monitor.getCurrent(QPMonitor::Metric::IAR) == 0.0)
            {
                int QARtrending = signals.QARtrending();
                int IARtrending = signals.IARtrending();
                int QQStrending = signals.QQStrending();
                int BORtrending = signals.BORtrending();

                /* Bucket Outdated anaylse */
                if (!zeroBOF && qp->monitor.getCurrent(QPMonitor::Metric::BOM) >= borUpperBound && BORtrending > 0)
                {
                    borCorrection = true;
                    if (this->insertOuterLevel == iLevels.size() - 1)
                    {
                        cout << "borcorrection lazy" << qp->monitor.getCurrent(QPMonitor::Metric::BOM) << endl;
                        this->queryOuterThreads = this->maxThreads;
                        this->insertOuterThreads = 0;
                        lazyInsertioActived = true;
                    } else {
                        cout << "borcorrection " << qp->monitor.getCurrent(QPMonitor::Metric::BOM) << endl;
                        bool c = incrementInsertionResourcesLevels(2);
                        if (c) {
                            forcedInsertionIncrease = true;
                        }
                    }
                } else 
                {
                    if (!zeroBOF 
                        && BORtrending <= 0
                        && qp->monitor.getCurrent(QPMonitor::Metric::BOM) <= borLowerBound
                        && borCorrection)
                        {
                            if (!lazyInsertioActived) {
                                cout << "borcorrection back " << qp->monitor.getCurrent(QPMonitor::Metric::BOM) << endl;
                                bool c = decrementInsertionResourcesLevels(1);
                                if (c) {
                                    forcedInsertionIncrease = false;
                                }
                            } else {
                                cout << "borcorrection lazy back " << qp->monitor.getCurrent(QPMonitor::Metric::BOM) << endl;
                                lazyInsertioActived = false;
                                this->insertOuterLevel = 0;
                                this->queryOuterLevel = qLevels.size() - 1;
                                this->queryInnerLevel = qLevels.at(this->queryOuterLevel).size() - 1;;

                                this->insertOuterThreads = iLevels.at(this->insertOuterLevel);
                                this->queryOuterThreads = qLevels.at(this->queryOuterLevel).at(this->queryInnerLevel);
                            }
                            borCorrection = false;
                        } else {
                            if (qp->monitor.getCurrent(QPMonitor::Metric::QQS) >= 1.0 && QQStrending > 0 && !borCorrection) {
                                cout << "QQSup " << qp->monitor.getCurrent(QPMonitor::Metric::QQS) << endl;
                                bool c = incrementQueryResourcesLevels(2);
                                    if (c) {
                                        fineGrainQueryIncrease = true;
                                    }
                            }

                            if (QQStrending == 0 && qp->monitor.getCurrent(QPMonitor::Metric::QQS) <= 0.01 
                            && fineGrainQueryIncrease) {
                                cout << "QQSdown " << qp->monitor.getCurrent(QPMonitor::Metric::QQS) << endl;
                                bool c = decrementQueryResourcesLevels(1);
                                fineGrainQueryIncrease = false;
                            }
                        }
                }
            
            }
    
            // 3. Reconfigure System
            if (this->insertOuterThreads != qp->insertOuterThreadsConfig ||
                this->queryOuterThreads != qp->queryOuterThreadsConfig.size()) {

                if (this->queryOuterThreads + this->insertOuterThreads > this->maxThreads) {
                    // ERROR
                    cout << "Some Inconsistent State: " << this->queryOuterThreads << " " << this->insertOuterThreads << endl;
                    exit(1);
                }

                qp->streamControllerStop = true;
                this->setupNewConfiguration(this->queryOuterThreads, this->insertOuterThreads);
                while(qp->streamControllerStop) {}

                qp->monitor.set(QPMonitor::Metric::CONF, this->queryOuterThreads + (0.1 * this->insertOuterThreads));
                qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
                qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
                qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);
            }

            // Log and Reset Monitor Metrics
            qp->monitor.log(w->msc_test_stream_controler_out("global").c_str());
            qp->monitor.reset(QPMonitor::Metric::QAR);
            qp->monitor.reset(QPMonitor::Metric::QQS);
            qp->monitor.reset(QPMonitor::Metric::IAR);
            qp->monitor.reset(QPMonitor::Metric::IQS);
            qp->monitor.reset(QPMonitor::Metric::QUW);
            qp->monitor.reset(QPMonitor::Metric::BOR);
            qp->monitor.reset(QPMonitor::Metric::BOM);
            currentTime = Time::getCurrentTime();
        }

        if (Time::getCurrentTime() - currentTimeTemporalUpdate > w->config.TEMPORALUPDATEINTERVAL) {
            qp->temporalIndex.temporalUpdate();
            currentTimeTemporalUpdate = Time::getCurrentTime();
        }
    }
}

void StreamController::msAdaptLevel2()
{
    auto *qp = dynamic_cast<QueryProcessor *>(w);
    if (qp->config.MSTEST) { //AQUI!!
        qp->wait_all->count_down_and_wait(); //AQUI!!
    } //AQUI!!

    // Parameters
    double  samplingInterval            = 0.05; //s
    double  currentTime                 = Time::getCurrentTime();
    double  currentTimeTemporalUpdate   = Time::getCurrentTime();
    bool    fineGrainQueryIncrease      = false;
    bool    borCorrection               = false;
    bool    lazyInsertioActived         = false;
    bool    forcedInsertionIncrease     = false;
    bool    earlyInsertionIncrease      = false;
    double  deltaBOM                    = 0.0;
    bool    zeroBOF                     = false;
    // Initializing
    SCSignals signals;
    signals.setup(&qp->monitor);

    // BOR
    double t = qp->temporalInsertionsBuffer.getFlexibility();
    double borUpperBound = t * 0.5;
    double borLowerBound = borUpperBound * 0.75;

    if (t <= 0) {
        zeroBOF = true;
    }

    qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
    qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
    qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);
    qp->monitor.set(QPMonitor::Metric::CONF, this->queryOuterThreads + (0.1 * this->insertOuterThreads));

    while(!qp->queryProcessorCanFinalize && !qp->insertProcessorCanFinalize) {
        qp->monitor.add(QPMonitor::Metric::QQS, qp->queriesToProcessBufferList.size());
        qp->monitor.add(QPMonitor::Metric::BOR, qp->temporalInsertionsBuffer.getOutdatedRatio());
        qp->monitor.add(QPMonitor::Metric::BOM, qp->temporalInsertionsBuffer.getOutdatedMax());
        qp->monitor.add(QPMonitor::Metric::IQS, qp->temporalInsertionsBuffer.getSize());

        qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
        qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
        qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);

        if (Time::getCurrentTime() - currentTime > samplingInterval) {
            samplingInterval = 0.05;
            if (!qp->monitor.getCurrent(QPMonitor::Metric::QAR) == 0.0
                && !qp->monitor.getCurrent(QPMonitor::Metric::IAR) == 0.0)
            {
                int QARtrending = signals.QARtrending();
                int IARtrending = signals.IARtrending();
                int QQStrending = signals.QQStrending();
                int BORtrending = signals.BORtrending();

                if (deltaBOM == 0.0) {
                    deltaBOM = qp->monitor.getCurrent(QPMonitor::Metric::BOM);
                } else {
                    deltaBOM = qp->monitor.getCurrent(QPMonitor::Metric::BOM) - qp->monitor.getOld(QPMonitor::Metric::BOM);
                }

                /* Bucket Outdated anaylse */
                if (!zeroBOF && qp->monitor.getCurrent(QPMonitor::Metric::BOM) >= borUpperBound && BORtrending > 0)
                {
                    borLowerBound = qp->monitor.getCurrent(QPMonitor::Metric::BOM) * 0.5;
                    // WARNING!! Achieved flexibility threshold.
                    // Force increase outer insertion threads
                    borCorrection = true;
                    // We have resources? 
                    if (this->insertOuterLevel == iLevels.size() - 1)
                    {
                        //thres no more resources! PANIC!
                        // Active lazy insertions Now!
                        this->queryOuterThreads = this->maxThreads;
                        this->insertOuterThreads = 0;
                        lazyInsertioActived = true;
                    } else {
                        bool c = incrementInsertionResourcesLevels(1);
                        if (c) {
                            forcedInsertionIncrease = true;
                        }
                    }
                }

                if (!zeroBOF 
                    && BORtrending <= 0
                    && qp->monitor.getCurrent(QPMonitor::Metric::BOM) <= borLowerBound
                    && borCorrection)
                    {
                        if (!lazyInsertioActived) {
                            bool c = decrementInsertionResourcesLevels(1);
                            if (c) {
                                forcedInsertionIncrease = false;
                            }
                        } else {
                            lazyInsertioActived = false;
                            this->insertOuterLevel = 0;
                            this->queryOuterLevel = qLevels.size() - 1;
                            this->queryInnerLevel = qLevels.at(this->queryOuterLevel).size() - 1;;

                            this->insertOuterThreads = iLevels.at(this->insertOuterLevel);
                            this->queryOuterThreads = qLevels.at(this->queryOuterLevel).at(this->queryInnerLevel);
                        }
                        borCorrection = false;
                    }

                switch(QARtrending)
                {
                    // Query Arrival Rate Increased? 50%
                    case 1:
                    {
                        // Check if we have resources
                        //if (this->maxThreads > this->insertOuterThreads + this->queryOuterThreads) {
                        if (!borCorrection) {
                            bool c = incrementQueryResourcesLevels(1);
                            if (c) {
                                fineGrainQueryIncrease = true;
                            }
                        }
                        qp->monitor.reset(QPMonitor::Metric::QST);
                        break;
                    }

                    // Query Arrival Rate Decreased? -50%
                    case -1:
                    {
                        // We can give some Query outer resources when:
                        // 1) Query Queue are empty
                        // 1) Query Queue trending are down and QAR are down to
                        if(qp->monitor.getCurrent(QPMonitor::Metric::QQS) <= 0.01) {
                            // Decrease query resources based on arrival change factor
                            // Slowly
                            bool c = decrementQueryResourcesLevels(1);
                        }
                        qp->monitor.reset(QPMonitor::Metric::QST);
                        fineGrainQueryIncrease = false;
                        break;
                    }

                    // Query Arrival Stable?
                    case 0:
                    {
                        // Query Queue keeping increasing?
                        if (QQStrending > 0) {
                            // Increase query resources
                            // Check if we have resources
                            //if (this->maxThreads > this->insertOuterThreads + this->queryOuterThreads) {
                                // We gonna take from inner threads
                            if (!borCorrection) {
                                bool c = incrementQueryResourcesLevels(1);
                                if (c) {
                                    fineGrainQueryIncrease = true;
                                }
                            }
                        }

                        // Query Queue Stable?
                        if (QQStrending == 0) {
                            // Our system througput is high then arrival rate? 
                            if (qp->monitor.getCurrent(QPMonitor::Metric::QQS) <= 0.01 && !fineGrainQueryIncrease) {
                                // Decrement slowly query resources
                                bool c = decrementQueryResourcesLevels(1);
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }

                switch(IARtrending)
                {
                    case 1:
                    {
                        if (zeroBOF) {
                            bool c = incrementInsertionResourcesLevels(1);
                        }
                        qp->monitor.reset(QPMonitor::Metric::IST);
                        break;
                    }

                    case 0:
                    {
                        if (qp->monitor.getCurrent(QPMonitor::Metric::IQS) <= 0.01 
                            && !borCorrection) {
                                bool c = decrementInsertionResourcesLevels(1);
                        }
                        break;
                    }

                    case -1:
                    {
                        if (!borCorrection) {
                            bool c = decrementInsertionResourcesLevels(1);
                        }
                        qp->monitor.reset(QPMonitor::Metric::IST);
                        break;
                    }
                }
            }
    
            // 3. Reconfigure System
            if (this->insertOuterThreads != qp->insertOuterThreadsConfig ||
                this->queryOuterThreads != qp->queryOuterThreadsConfig.size()) {

                if (this->queryOuterThreads + this->insertOuterThreads > this->maxThreads) {
                    // ERROR
                    cout << "Some Inconsistent State: " << this->queryOuterThreads << " " << this->insertOuterThreads << endl;
                    exit(1);
                }

                qp->streamControllerStop = true;
                this->setupNewConfiguration(this->queryOuterThreads, this->insertOuterThreads);
                while(qp->streamControllerStop) {}

                qp->monitor.set(QPMonitor::Metric::CONF, this->queryOuterThreads + (0.1 * this->insertOuterThreads));
                qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
                qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
                qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);
            }

            // Log and Reset Monitor Metrics
            qp->monitor.log(w->msc_test_stream_controler_out("global").c_str());
            qp->monitor.reset(QPMonitor::Metric::QAR);
            qp->monitor.reset(QPMonitor::Metric::QQS);
            qp->monitor.reset(QPMonitor::Metric::IAR);
            qp->monitor.reset(QPMonitor::Metric::IQS);
            qp->monitor.reset(QPMonitor::Metric::QUW);
            qp->monitor.reset(QPMonitor::Metric::BOR);
            qp->monitor.reset(QPMonitor::Metric::BOM);
            currentTime = Time::getCurrentTime();
        }

        if (Time::getCurrentTime() - currentTimeTemporalUpdate > w->config.TEMPORALUPDATEINTERVAL) {
            qp->temporalIndex.temporalUpdate();
            currentTimeTemporalUpdate = Time::getCurrentTime();
        }
    }
}

bool StreamController::decrementInsertionResources(double decreaseFactor)
{
    // We can give Insertion Outer Threads to Query Stream, if have it.
    if (this->insertOuterThreads <= 1) {
        return false;
    }

    int ION = floor(decreaseFactor * this->insertOuterThreads);
    if (decreaseFactor == -1) {
        // Slowly decrease
        ION = this->insertOuterThreads - 1;
    }

    if (ION < 1) {
        ION = 1;
    }

    //cout << "[insertion] decrement: to " << ION << "(" << decreaseFactor << ") from " << this->insertOuterThreads << endl;
    // Upgrading Query Response Time
    this->insertOuterThreads = ION;
    return true;
}

bool StreamController::incrementInsertionResources(double increaseFactor)
{
    // We need to increment Insertion Outer Threads as much as possible to support new AIR
    if (this->insertOuterThreads == 0) {
        // Nothing to do Query resource maximum
        // No resources available
        return false;
    }
    if (this->insertOuterThreads == this->maxThreads - 1) {
        //Maximun insertion outer
        // ?? this->insertOuterThreads -= 1;
        return false;
    }
    int queryInner = this->maxThreads - (this->queryOuterThreads + this->insertOuterThreads);
    if (queryInner == 0) {
        // Nothing to do Query resource maximum
        // No resources available
        return false;
    }

    int ION = round(increaseFactor * this->insertOuterThreads);
    if (increaseFactor == -1) {
        // Slowly increase
        ION = this->insertOuterThreads + 1;
    }
    int diff_i = ION - this->insertOuterThreads;

    //cout << "[insertion] increment: to " << ION << "(" << increaseFactor << ") from " << this->insertOuterThreads << endl; 
    // If we can get threads from other stream, 1) it would be from Query Inner Threads
    // or if dont have resources to support AIR, 2) we need to turn on Lazy Insertions
    if (diff_i > queryInner) {
        // 2) Not enough resources
        if (queryInner > 0) {
            // Much as possible
            this->insertOuterThreads += queryInner;
        }
    } else {
        // 1) Sacrificing Query Response Time
        // We dont need Lazy Insertions
        this->insertOuterThreads = ION;
    }
    return true;
}

bool StreamController::decrementQueryResources(double decreaseFactor)
{
    // We can give some outer query threads to:
    // 1) Insertion Outer Threads or 2) Query Response Rate
    if (this->queryOuterThreads == 1) {
        return false;
    }

    int QON = round(decreaseFactor * this->queryOuterThreads);
    //cout << "[query] decrement: to " << QON << "(" << decreaseFactor << ") from " << this->queryOuterThreads << endl; 
    if (decreaseFactor == -1) {
        //Decrease Slowly
        QON = this->queryOuterThreads - 1;
    }
    if (QON < 1) {
        QON = 1;
    }
    /*if (this->insertOuterThreads == 0) {
        // 1) Active Eager Insertions
        this->insertOuterThreads = 1;
        this->queryOuterThreads = QON - 1;
        if (this->queryOuterThreads <= 0) {
            this->queryOuterThreads = 1;
        }
    } else {*/
        // 2) Upgrading Query Response Time
        this->queryOuterThreads = QON;
    //}
    return true;
}

bool StreamController::incrementQueryResources(double increaseFactor) 
{
    int QON = round(increaseFactor * this->queryOuterThreads); 
    if (increaseFactor == -1) {
        QON = this->queryOuterThreads + 1;
    }

    if (QON == this->queryOuterThreads) {
        return false;
    }

    if (this->queryOuterThreads == this->maxThreads) {
        return false;
    }

    //cout << "[query] increment: to " << QON << "(" << increaseFactor << ") from " << this->queryOuterThreads << endl;
    if (QON >= this->maxThreads) {
        // Not enough resources
        // Active Lazy Insertions
        // [MaxThreads][Min][0]
        this->queryOuterThreads = this->maxThreads;
        this->insertOuterThreads = 0;
    } else {
        // We have resources, however we need to take from other stream:
        // 1) Try to get resources from Query Inner Threads
        // 2) Try to get resources from Insertion Outer Threads
        int QT = this->maxThreads - this->insertOuterThreads;
        if (QON <= QT) {
            // 1) Sacrificing Query Response Time
            // [QON][rest][same]
            this->queryOuterThreads = QON;
        } else {
            // 2) Sacrificing Insert Throughput
            // [QON][Min][ION]
            int ION = this->insertOuterThreads - (QON - QT);
            this->queryOuterThreads = QON;
            this->insertOuterThreads = ION;
        }
    }
    return true;
}

void StreamController::msAdapt()
{
    auto *qp = dynamic_cast<QueryProcessor *>(w);
    if (qp->config.MSTEST) { //AQUI!!
        qp->wait_all->count_down_and_wait(); //AQUI!!
    } //AQUI!!

    // Parameters
    double  samplingInterval            = 0.05; //s
    double  currentTime                 = Time::getCurrentTime();
    double  currentTimeTemporalUpdate   = Time::getCurrentTime();
    bool    fineGrainQueryIncrease      = false;
    bool    borCorrection               = false;
    bool    lazyInsertioActived         = false;
    bool    forcedInsertionIncrease     = false;
    bool    earlyInsertionIncrease      = false;
    double  deltaBOM                    = 0.0;
    // Initializing
    SCSignals signals;
    signals.setup(&qp->monitor);

    // BOR
    double t = qp->temporalInsertionsBuffer.getFlexibility();
    double borUpperBound = t * 0.5;
    double borLowerBound = borUpperBound * 0.75;

    qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
    qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
    qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);
    qp->monitor.set(QPMonitor::Metric::CONF, this->queryOuterThreads + (0.1 * this->insertOuterThreads));

    while(!qp->queryProcessorCanFinalize && !qp->insertProcessorCanFinalize) {
        qp->monitor.add(QPMonitor::Metric::QQS, qp->queriesToProcessBufferList.size());
        qp->monitor.add(QPMonitor::Metric::BOR, qp->temporalInsertionsBuffer.getOutdatedRatio());
        qp->monitor.add(QPMonitor::Metric::BOM, qp->temporalInsertionsBuffer.getOutdatedMax());
        qp->monitor.add(QPMonitor::Metric::IQS, qp->temporalInsertionsBuffer.getSize());

        qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
        qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
        qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);

        if (Time::getCurrentTime() - currentTime > samplingInterval) {
            samplingInterval = 0.05;
            if (!qp->monitor.getCurrent(QPMonitor::Metric::QAR) == 0.0
                && !qp->monitor.getCurrent(QPMonitor::Metric::IAR) == 0.0)
            {
                int QARtrending = signals.QARtrending();
                int IARtrending = signals.IARtrending();
                int QQStrending = signals.QQStrending();
                int BORtrending = signals.BORtrending();

                if (deltaBOM == 0.0) {
                    deltaBOM = qp->monitor.getCurrent(QPMonitor::Metric::BOM);
                } else {
                    deltaBOM = qp->monitor.getCurrent(QPMonitor::Metric::BOM) - qp->monitor.getOld(QPMonitor::Metric::BOM);
                }

                /* Bucket Outdated anaylse */
                /*if (qp->monitor.getCurrent(QPMonitor::Metric::BOM) + deltaBOM >= t && BORtrending > 0)*/
                if (qp->monitor.getCurrent(QPMonitor::Metric::BOM) >= borUpperBound && BORtrending > 0)
                {
                    borLowerBound = qp->monitor.getCurrent(QPMonitor::Metric::BOM) * 0.5;
                    // WARNING!! Achieved flexibility threshold.
                    // Force increase outer insertion threads
                    borCorrection = true;
                    //cout << "[BORCORRECTION ATIVED]: ";
                    //TEST  if (earlyInsertionIncrease) {
                    //TEST        earlyInsertionIncrease = false;
                    //TEST  }
                    // We have resources? 
                    if (this->maxThreads > this->insertOuterThreads + this->queryOuterThreads)
                    {
                        // We can increment directly
                        //cout << "inner" << endl;
                        this->insertOuterThreads += 1;
                    } else {
                        // Its bad. We need to take from query outer threads
                        // But.. 
                        if (this->insertOuterThreads == this->maxThreads - 1)
                        {
                            //thres no more resources! PANIC!
                            // Active lazy insertions Now!
                            this->queryOuterThreads = this->maxThreads;
                            this->insertOuterThreads = 0;
                            lazyInsertioActived = true;
                            //cout << "lazy" << endl;
                        } else {
                            //cout << "forced" <<endl;
                            // Getting from query outer threads
                            this->insertOuterThreads += 1;
                            this->queryOuterThreads -= 1;
                            forcedInsertionIncrease = true;
                        }
                    }
                }

                if (BORtrending <= 0
                    && qp->monitor.getCurrent(QPMonitor::Metric::BOM) <= borLowerBound
                    && borCorrection)
                    {
                        //cout << "[BORCORRECTION DESACTIVED]: ";
                        if (!lazyInsertioActived) {
                            if (forcedInsertionIncrease) {
                                //cout << " undo forced" <<endl;
                                this->insertOuterThreads -= 1;
                                this->queryOuterThreads += 1;
                                forcedInsertionIncrease = false;
                            } else {
                                //cout << " undo inner" <<endl;
                                this->insertOuterThreads -= 1;
                            }
                        } else {
                            //cout << " undo lazy" <<endl;
                            lazyInsertioActived = false;
                            if (this->maxThreads == this->queryOuterThreads) {
                                this->queryOuterThreads -= 1;
                            }
                            this->insertOuterThreads = 1;
                        }
                        borCorrection = false;
                    }

                switch(QARtrending)
                {
                    // Query Arrival Rate Increased? 50%
                    case 1:
                    {
                        // Check if we have resources
                        if (this->maxThreads > this->insertOuterThreads + this->queryOuterThreads) {
                        //TEST  || earlyInsertionIncrease) {
                            // We gonna take from inner threads
                            bool c = incrementQueryResources(-1);
                            if (c) {
                                //cout << "[query increased by QARtrending=1 e borCorrection]" << endl;
                                fineGrainQueryIncrease = true;
                                //TEST   if (earlyInsertionIncrease) {
                                //TEST      earlyInsertionIncrease = false;
                                //TEST  }
                            }
                        }
                        qp->monitor.reset(QPMonitor::Metric::QST);
                        break;
                    }

                    // Query Arrival Rate Decreased? -50%
                    case -1:
                    {
                        // We can give some Query outer resources when:
                        // 1) Query Queue are empty
                        // 1) Query Queue trending are down and QAR are down to
                        if(qp->monitor.getCurrent(QPMonitor::Metric::QQS) <= 0.01) {
                            // Decrease query resources based on arrival change factor
                            // Slowly
                            bool c = decrementQueryResources(-1);
                            //if (c) {
                            //    cout << "[query decreased by QARtrending=-1 e QQS<=0]" << endl;
                            //}
                        }
                        qp->monitor.reset(QPMonitor::Metric::QST);
                        fineGrainQueryIncrease = false;
                        break;
                    }

                    // Query Arrival Stable?
                    case 0:
                    {
                        // Query Queue keeping increasing?
                        if (QQStrending > 0) {
                            // Increase query resources
                            // Check if we have resources
                            if (this->maxThreads > this->insertOuterThreads + this->queryOuterThreads) {
                            //TEST  || earlyInsertionIncrease) {
                                // We gonna take from inner threads
                                bool c = incrementQueryResources(-1);
                                if (c) {
                                    //cout << "[query increased by QQStrending=1 e borCorrection]" << endl;
                                    fineGrainQueryIncrease = true;
                                    //TEST  if (earlyInsertionIncrease) {
                                    //TEST     earlyInsertionIncrease = false;
                                    //TEST  }
                                }
                            }
                        }
                        // Query Queue keeping decreasing?
                        // Keep the configuration until theres no query on queue

                        // Query Queue Stable?
                        if (QQStrending == 0) {
                            // Our system througput is high then arrival rate? 
                            if (qp->monitor.getCurrent(QPMonitor::Metric::QQS) <= 0.01 && !fineGrainQueryIncrease) {
                                // Decrement slowly query resources
                                bool c = decrementQueryResources(-1);
                                //if (c) {
                                //   cout << "[query decreased by QQStrending==0 e QQS<=0]" << endl;
                                //}
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }

                switch(IARtrending)
                {
                    case 1:
                    {
                        qp->monitor.reset(QPMonitor::Metric::IST);
                        break;
                    }

                    case 0:
                    {
                        if (qp->monitor.getCurrent(QPMonitor::Metric::IQS) <= 0.01 
                            && !borCorrection) {
                                bool c= decrementInsertionResources(-1);
                                //if (c) {
                                //    cout << "[insertion decreased by IQS<=0 e !borCorrection]" << endl;
                                    //TEST  if (earlyInsertionIncrease) {
                                    //TEST      earlyInsertionIncrease = false;
                                    //TEST  }
                                //}
                        }
                        break;
                    }

                    case -1:
                    {
                        if (!borCorrection) {
                            bool c= decrementInsertionResources(-1);
                            //if (c) {
                                //cout << "[insertion decreased by IARtrending=-1 e !borCorrection]" << endl;
                                //TEST  if (earlyInsertionIncrease) {
                                //TEST      earlyInsertionIncrease = false;
                                //TEST  }
                            //}
                        }
                        qp->monitor.reset(QPMonitor::Metric::IST);
                        break;
                    }
                }
                //TEST  if (BORtrending > 0 && 
                //TEST      this->maxThreads > this->insertOuterThreads + this->queryOuterThreads && 
                //TEST      !borCorrection && !earlyInsertionIncrease)
                //TEST  {   
                //TEST      this->insertOuterThreads += 1;
                //TEST     earlyInsertionIncrease = true;
                //TEST      cout << "[insertion increased by earlyInsertion]" << endl;
                //TEST  }

                //TEST  if (qp->monitor.getCurrent(QPMonitor::Metric::BOM) <= 0)
                //TEST  {
                //TEST      this->insertOuterThreads -= 1;
                //TEST      if (earlyInsertionIncrease) {
                //TEST          earlyInsertionIncrease = false;
                //TEST      }
                //TEST       cout << "[insertion increased by lowerlimit]" << endl;
                //TEST  }
            }
    
            // 3. Reconfigure System
            if (this->insertOuterThreads != qp->insertOuterThreadsConfig ||
                this->queryOuterThreads != qp->queryOuterThreadsConfig.size()) {

                if (this->queryOuterThreads + this->insertOuterThreads > this->maxThreads) {
                    // ERROR
                    cout << "Some Inconsistent State: " << this->queryOuterThreads << " " << this->insertOuterThreads << endl;
                    exit(1);
                }

                qp->streamControllerStop = true;
                this->setupNewConfiguration(this->queryOuterThreads, this->insertOuterThreads);
                while(qp->streamControllerStop) {}

                qp->monitor.set(QPMonitor::Metric::CONF, this->queryOuterThreads + (0.1 * this->insertOuterThreads));
                qp->monitor.set(QPMonitor::Metric::T, qp->temporalInsertionsBuffer.getFlexibility());
                qp->monitor.set(QPMonitor::Metric::TLB, borLowerBound);
                qp->monitor.set(QPMonitor::Metric::TUB, borUpperBound);
            }

            // Log and Reset Monitor Metrics
            qp->monitor.log(w->msc_test_stream_controler_out("global").c_str());
            qp->monitor.reset(QPMonitor::Metric::QAR);
            qp->monitor.reset(QPMonitor::Metric::QQS);
            qp->monitor.reset(QPMonitor::Metric::IAR);
            qp->monitor.reset(QPMonitor::Metric::IQS);
            qp->monitor.reset(QPMonitor::Metric::QUW);
            qp->monitor.reset(QPMonitor::Metric::BOR);
            qp->monitor.reset(QPMonitor::Metric::BOM);
            currentTime = Time::getCurrentTime();
        }

        if (Time::getCurrentTime() - currentTimeTemporalUpdate > w->config.TEMPORALUPDATEINTERVAL) {
            qp->temporalIndex.temporalUpdate();
            currentTimeTemporalUpdate = Time::getCurrentTime();
        }
    }
}

void StreamController::run()
{
    auto *qp = dynamic_cast<QueryProcessor *>(w);
    if (qp->config.MSTEST) { //AQUI!!
        qp->wait_all->count_down_and_wait(); //AQUI!!
    } //AQUI!!
        
    // Parameters
    double samplingInterval = 0.05; //s
    double currentTime = Time::getCurrentTime();
    double currentTimeTemporalUpdate = Time::getCurrentTime();

    // Initializing
    SCSignals signals;
    signals.setup(&qp->monitor);

    while(!qp->queryProcessorCanFinalize && !qp->insertProcessorCanFinalize) {
        qp->monitor.add(QPMonitor::Metric::QQS, qp->queriesToProcessBufferList.size());
        qp->monitor.add(QPMonitor::Metric::BOR, qp->temporalInsertionsBuffer.getOutdatedRatio());
        qp->monitor.add(QPMonitor::Metric::BOM, qp->temporalInsertionsBuffer.getOutdatedMax());
        qp->monitor.add(QPMonitor::Metric::IQS, qp->temporalInsertionsBuffer.getSize());

        if (Time::getCurrentTime() - currentTime > samplingInterval) {
            if (!qp->monitor.getCurrent(QPMonitor::Metric::QAR) == 0.0
                && !qp->monitor.getCurrent(QPMonitor::Metric::IAR) == 0.0
                && !qp->monitor.getCurrent(QPMonitor::Metric::QST) == 0.0
                && !qp->monitor.getCurrent(QPMonitor::Metric::IST) == 0.0) 
            {
                int QARtrending = signals.QARtrending();
                int IARtrending = signals.IARtrending();
                int QQStrending = signals.QQStrending();
                int BORtrending = signals.BORtrending();
            }
            // Log and Reset Monitor Metrics
            qp->monitor.log(w->msc_test_stream_controler_out("global").c_str());
            qp->monitor.reset(QPMonitor::Metric::QAR);
            qp->monitor.reset(QPMonitor::Metric::QQS);
            qp->monitor.reset(QPMonitor::Metric::IAR);
            qp->monitor.reset(QPMonitor::Metric::IQS);
            qp->monitor.reset(QPMonitor::Metric::QUW);
            qp->monitor.reset(QPMonitor::Metric::BOR);
            qp->monitor.reset(QPMonitor::Metric::BOM);
            currentTime = Time::getCurrentTime();
        }

        if (Time::getCurrentTime() - currentTimeTemporalUpdate > w->config.TEMPORALUPDATEINTERVAL) {
            qp->temporalIndex.temporalUpdate();
            currentTimeTemporalUpdate = Time::getCurrentTime();
        }
    }
}
void StreamController::setupNewConfiguration(int qOuterThreads, int insertInnerThreads)
{
    if (qOuterThreads > this->maxThreads || insertInnerThreads > this->maxThreads) {
        //ERROR
        return setupNewConfiguration(1, 1);
    }

    auto *qp = dynamic_cast<QueryProcessor *>(w);
    qp->queryOuterThreadsConfig.clear();

    qp->updatedCheckStart = qp->config.OUTERINSERTTHREADS == 0;

    this->insertOuterThreads = insertInnerThreads;
    if (qOuterThreads == this->maxThreads) {
        this->insertOuterThreads = 0;
    }
    qp->insertOuterThreadsConfig = this->insertOuterThreads;

    this->queryOuterThreads = qOuterThreads;
    if (qp->config.OUTERQUERYTHREADS <= 0) {
        qp->queryOuterThreadsConfig.clear();
        qp->monitor.configure(0, this->insertOuterThreads);
        return;
    }

    int totalQueryInner = this->maxThreads - this->insertOuterThreads;
    int distributedInnerThreads = 0;
    int coreOffset = qp->config.DEDICATEDCORES + this->insertOuterThreads;

    for (int outer = 0; outer < this->queryOuterThreads - 1; outer++) {
        int perOuterQueryInner = ceil((double)(totalQueryInner - distributedInnerThreads)/(double)(this->queryOuterThreads - outer));

        qp->queryOuterThreadsConfig.emplace_back(perOuterQueryInner, coreOffset);
        distributedInnerThreads += perOuterQueryInner;
        coreOffset += perOuterQueryInner;
    }

    qp->queryOuterThreadsConfig.emplace_back(totalQueryInner - distributedInnerThreads, coreOffset);

    qp->monitor.configure(this->queryOuterThreads, this->insertOuterThreads);
    cout << "[" <<  this->queryOuterThreads << "][" << (double) totalQueryInner / (double) this->queryOuterThreads << "][" << this->insertOuterThreads << "]" << endl;
}