namespace Mach.Utility
{
	internal static class Constants
	{
		/// <summary>
		/// A sentinel value indicating no (or infinite) timeout.
		/// </summary>
		public const int InfiniteTimeout = int.MaxValue;
        public const int DefaultFetchSize = 3000;
        public const int NetworkSuccessRatio = 90; // 0 ~ 100, for testing
	}
}
