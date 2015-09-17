package com.spikeify.taskqueue.entities;

public class TaskStatistics {

	protected long count;

	protected long minJobRunTime;
	protected long maxJobRunTime;
	protected long totalJobRunTime;

	protected long minExecutionTime;
	protected long maxExecutionTime;
	protected long totalExecutionTime;

	protected long averageJobRunTime;
	protected long averageExecutionTime;

	public long getCount() {

		return count;
	}

	public long getMinJobRunTime() {

		return minJobRunTime;
	}

	public long getMaxJobRunTime() {

		return maxJobRunTime;
	}

	public long getTotalJobRunTime() {

		return totalJobRunTime;
	}

	public long getMinExecutionTime() {

		return minExecutionTime;
	}

	public long getMaxExecutionTime() {

		return maxExecutionTime;
	}

	public long getTotalExecutionTime() {

		return totalExecutionTime;
	}

	public long getAverageJobRunTime() {

		return averageJobRunTime;
	}

	public long getAverageExecutionTime() {

		return averageExecutionTime;
	}

	public static class Builder {

		long count;

		Long minJobRunTime = null;
		Long maxJobRunTime = null;
		Long minExecutionTime = null;
		Long maxExecutionTime = null;

		long totalJobRunTime;
		long totalExecutionTime;

		long averageJobRunTime;
		long averageExecutionTime;

		private void calculateAverage() {

			if (count > 0) {

				averageJobRunTime = totalJobRunTime / count;
				averageExecutionTime = totalExecutionTime / count;
			}
		}


		public void include(QueueTask item) {

			if (item != null) {

				setMinMaxExecutionTime(item.getExecutionTime());
				setMinMaxJobRunTime(item.getJobRunTime());

				if (item.getExecutionTime() != null) {
					totalExecutionTime = totalExecutionTime + item.getExecutionTime();
				}

				if (item.getJobRunTime() != null) {
					totalJobRunTime = totalJobRunTime + item.getJobRunTime();
				}

				count++;
			}
		}

		public Builder include(TaskStatistics old) {

			if (old == null)
				return this;

			if (old.count > 0) {

				totalJobRunTime = totalJobRunTime + old.getTotalJobRunTime();
				totalExecutionTime = totalExecutionTime + old.getTotalExecutionTime();
				count = count + old.count;

				setMinMaxExecutionTime(old.getMinExecutionTime());
				setMinMaxExecutionTime(old.getMaxExecutionTime());

				setMinMaxJobRunTime(old.getMinJobRunTime());
				setMinMaxJobRunTime(old.getMaxJobRunTime());
			}

			return this;
		}

		/**
		 * calculates average according to included items
		 * @return statistics or null if no items were included
		 */
		public TaskStatistics build() {

			if (count == 0) {
				return null;
			}

			calculateAverage();
			return getTaskStatistics();
		}

		/**
		 * Joins previous statistics with current
		 *
		 * @param previous calculated statistics
		 */
		public TaskStatistics buildWith(TaskStatistics previous) {

			if (count > 0 && previous.count > 0) {

				include(previous);
				calculateAverage();
			}

			return getTaskStatistics();
		}

		private void setMinMaxExecutionTime(Long jobExecutionTime) {

			if (jobExecutionTime != null) {
				if (minExecutionTime == null || jobExecutionTime < minExecutionTime) {
					minExecutionTime = jobExecutionTime;
				}

				if (maxExecutionTime == null || jobExecutionTime > maxExecutionTime) {
					maxExecutionTime = jobExecutionTime;
				}
			}
		}

		private void setMinMaxJobRunTime(Long jobRunTime) {

			if (jobRunTime != null) {
				if (minJobRunTime == null || jobRunTime < minJobRunTime) {
					minJobRunTime = jobRunTime;
				}

				if (maxJobRunTime == null || jobRunTime > maxJobRunTime) {
					maxJobRunTime = jobRunTime;
				}
			}
		}

		private TaskStatistics getTaskStatistics() {

			TaskStatistics statistics = new TaskStatistics();
			statistics.count = count;

			statistics.maxExecutionTime = maxExecutionTime != null ? maxExecutionTime : 0;
			statistics.minExecutionTime = minExecutionTime != null ? minExecutionTime : 0;
			statistics.averageExecutionTime = averageExecutionTime;
			statistics.totalExecutionTime = totalExecutionTime;

			statistics.minJobRunTime = minJobRunTime != null ? minJobRunTime : 0;
			statistics.maxJobRunTime = maxJobRunTime != null ? maxJobRunTime : 0;
			statistics.averageJobRunTime = averageJobRunTime;
			statistics.totalJobRunTime = totalJobRunTime;
			return statistics;
		}
	}
}
