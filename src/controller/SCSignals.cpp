//
// Created by gnandrade on 25/04/2021.
//

#include "controller/SCSignals.h"

void SCSignals::setup(QPMonitor *monitor)
{
    this->monitor = monitor;
    QARbefore = 0.0;
    QQSbefore = 0.0;
    IARbefore = 0.0;
    BORbefore = 0.0;
}

int SCSignals::QARtrending()
{
    // Checking Query Arrival Rate Trending
    int QARtrend = 0;
    double QAR = monitor->getCurrent(QPMonitor::Metric::QAR);
    if (QARbefore != 0.0) {
        if (QAR > QARbefore) {
            double increased = (QAR - QARbefore) / QAR;
            if (increased >= QAROffset) {
                QARtrend = 1;
                QARbefore = QAR;
            }
        }
        if (QAR < QARbefore) {
            double decreased = (QARbefore - QAR) / QARbefore;
            if (decreased >= QAROffset) {
                QARtrend = -1;
                QARbefore = QAR;
            }
        }
        monitor->setTrend(QPMonitor::Metric::QAR, QARtrend);
    } else {
        QARbefore = monitor->getCurrent(QPMonitor::Metric::QAR);
    }

    return QARtrend;
}

int SCSignals::IARtrending()
{
    // Checking Insertion Arrival Rate Trending
    int IARtrend = 0;
    double IAR = monitor->getCurrent(QPMonitor::Metric::IAR);
    if (IARbefore != 0.0) {
        if (IAR > IARbefore) {
            double increased = (IAR - IARbefore) / IAR;
            if (increased >= IAROffset) {
                IARtrend = 1;
                IARbefore = IAR;
            }
        }
        if (IAR < IARbefore) {
            double decreased = (IARbefore - IAR) / IARbefore;
            if (decreased >= IAROffset) {
                IARtrend = -1;
                IARbefore = IAR;
            }
        }
        monitor->setTrend(QPMonitor::Metric::IAR, IARtrend);
    } else {
        IARbefore = monitor->getCurrent(QPMonitor::Metric::IAR);
    }

    return IARtrend;
}

int SCSignals::QQStrending()
{
    // Checking Query Queue Size Trending
    int QQStrend = 0;
    double QQS = monitor->getCurrent(QPMonitor::Metric::QQS);
    if (QQSbefore != 0.0) {
        if (QQS  > QQSbefore + QQSOffset) {
            QQStrend = 1;
            QQSbefore = QQS;
        }
        if (QQS + QQSOffset < QQSbefore) {
            QQStrend = -1;
            QQSbefore = QQS;
        }
        monitor->setTrend(QPMonitor::Metric::QQS, QQStrend);
    } else {
        QQSbefore = monitor->getCurrent(QPMonitor::Metric::QQS);
    }

    return QQStrend;
}

int SCSignals::BORtrending()
{
    // Checking Bucket outdated ratio Trending
    int BORtrend = 0;
    double BOR = monitor->getCurrent(QPMonitor::Metric::BOR);
    if (BORbefore != 0.0) {
        if (BOR > BORbefore + BOROffset) {
            BORtrend = 1;
            BORbefore = BOR;
        }
        if (BOR + BOROffset < BORbefore) {
            BORtrend = -1;
            BORbefore = BOR;
        }
        monitor->setTrend(QPMonitor::Metric::BOR, BORtrend);
    } else {
        BORbefore = monitor->getCurrent(QPMonitor::Metric::BOR);
    }

    return BORtrend;
}