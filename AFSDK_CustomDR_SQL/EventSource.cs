using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.Time;

namespace AFSDK_CustomDR_SQL
{
    /*****************************************************************************************
     * The EventSource class implements AFEventSource to specify how to get data pipe events 
     * from the system of record. 
     *****************************************************************************************/
    class EventSource : AFEventSource
    {
        // Last timestamps for each AF Attribute
        Dictionary<AFAttribute, AFTime> _lastTimes = new Dictionary<AFAttribute, AFTime>();

        // Start time when the pipe is initiated
        AFTime _startTime;
        
        // Initialize the start time for the event source
        public EventSource()
        {
            _startTime = new AFTime("*");
        }

        // Get new events for the pipe from the last timestamps till current time of evaluation
        protected override bool GetEvents()
        {
            // Set evaluation time to current time
            AFTime evalTime = AFTime.Now;

            // Get the list of AF Attributes signed up on the data pipe
            IEnumerable<AFAttribute> signupList = base.Signups;

            // Get values for each AF Attribute, one at a time
            foreach (AFAttribute att in signupList)
            {
                if (!ReferenceEquals(att, null))
                {
                    // Add AF Attribute if it hasn't been added to the _lastTimes dictionary yet
                    if (!_lastTimes.ContainsKey(att))
                    {
                        _lastTimes.Add(att, this._startTime);
                    }

                    // Set time range to get all values between last timestamps to current evaluation time
                    AFTimeRange timeRange = new AFTimeRange(_lastTimes[att], evalTime);

                    /* Note: Use RecordedValues if supported. GetValues call return interpolated values at the start and end time,
                     * which can be problematic in a data pipe implementation. GetValues is used here for this simple example because
                     * the implementation of GetValues in my custom DR does not return interpolated values at the start and end time. */
                    AFValues vals = att.GetValues(timeRange, 0, att.DefaultUOM);

                    // Store old last time for the AF Attribute
                    AFTime lastTime = _lastTimes[att];

                    // Publish each value to the data pipe
                    foreach (AFValue val in vals)
                    {
                        // Record latest timestamp
                        if (val.Timestamp > lastTime)
                        {
                            lastTime = val.Timestamp;
                        }
                        AFDataPipeEvent ev = new AFDataPipeEvent(AFDataPipeAction.Add, val);
                        base.PublishEvent(att, ev);
                    }

                    // Add a tick to the latest time stamp to prevent the next GetValues call from returning value at the same time
                    _lastTimes[att] = lastTime + TimeSpan.FromTicks(1);
                }
            }
            return false;
        }

        // Dispose resources
        protected override void Dispose(bool disposing)
        {
            _lastTimes = null;
        }
    }
}
