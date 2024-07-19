using Microsoft.EntityFrameworkCore;

namespace EFCore.CannotAggregationInSubQuery.SQL.Error.Sample
{
    public class TableName
    {
        public int Id { get; set; }
        public DateTime? IncidentDate { get; set; }
        public DateTime? ReportReceived { get; set; }
        public DateTime? InitiatedDate { get; set; }
        public DateTime? FinalizedDate { get; set; }
        public string ClaimId { get; set; }
        public string ClaimIdentifier { get; set; }
        public UnderwritingPeriod UnderwritingPeriod { get; set; }
    }

    public class Reserve
    {
        public int Id { get; set; }
        public string ClaimId { get; set; }
        public string ReserveType { get; set; }
        public DateTime TransactionDate { get; set; }
    }

    public class UnderwritingPeriod
    {
        public int Id { get; set; }
        public string PolicyNumber1 { get; set; }
        public string PolicyNumber2 { get; set; }
        public string PolicyNumber3 { get; set; }
        public Policy Policy { get; set; }
    }

    public class Policy
    {
        public int Id { get; set; }
        public string PolicyIdentifier { get; set; }
    }

    public class ClaimsReactionsReportDto
    {
        public DateTime? IncidentDate { get; set; }
        public DateTime? ReportReceivedDate { get; set; }
        public DateTime? InitiatedDate { get; set; }
        public DateTime? FinalizedDate { get; set; }
        public DateTime? FirstReserveDate { get; set; }
        public DateTime? FirstPaymentDate { get; set; }
        public int? IncidentToReceivedDays { get; set; }
        public int? ReceivedToInitiatedDays { get; set; }
        public int? InitiatedToFirstReserveDays { get; set; }
        public int? InitiatedToFirstPaymentDays { get; set; }
        public int? InitiatedToFinalizedDays { get; set; }
        public string ClaimIdentifier { get; set; }
        public string Category { get; set; }
        public string PolicyIdentifier { get; set; }
    }

    public class ClaimsReactionsAVGReportDto
    {
        public double? IncidentToReceivedDaysAVG { get; set; }
        public double? ReceivedToInitiatedDaysAVG { get; set; }
        public double? InitiatedToFirstReserveDaysAVG { get; set; }
        public double? InitiatedToFirstPaymentDaysAVG { get; set; }
        public double? InitiatedToFinalizedDaysAVG { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public DateTime DateString { get; set; }
    }

    public class AppDbContext : DbContext
    {
        public DbSet<TableName> TableName { get; set; }
        public DbSet<Reserve> Reserves { get; set; }
        public DbSet<UnderwritingPeriod> UnderwritingPeriods { get; set; }
        public DbSet<Policy> Policies { get; set; }

        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Add any additional model configurations here
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting demo application...");

            // Setup DbContext
            var options = new DbContextOptionsBuilder<AppDbContext>()
                .UseSqlServer(@"Server=(localdb)\mssqllocaldb;Database=ClaimsDemoDb;Trusted_Connection=True;")
                .Options;

            using (var context = new AppDbContext(options))
            {
                // Ensure database is created
                Console.WriteLine("Creating database...");
                context.Database.EnsureCreated();

                // Seed data
                Console.WriteLine("Seeding data...");
                SeedData(context);

                // Run the problematic query
                Console.WriteLine("Running query...");
                RunProblematicQuery(context);
            }

            Console.WriteLine("Demo completed. Press any key to exit.");
            Console.ReadKey();
        }

        private static void SeedData(AppDbContext context)
        {
            if (!context.TableName.Any())
            {
                var policy = new Policy { PolicyIdentifier = "POL001" };
                context.Policies.Add(policy);

                var underwritingPeriod = new UnderwritingPeriod
                {
                    PolicyNumber1 = "PN001",
                    Policy = policy,
                    PolicyNumber2 = "PN001",
                    PolicyNumber3 = "PN003"
                };
                context.UnderwritingPeriods.Add(underwritingPeriod);

                var claim = new TableName
                {
                    IncidentDate = DateTime.Now.AddDays(-30),
                    ReportReceived = DateTime.Now.AddDays(-28),
                    InitiatedDate = DateTime.Now.AddDays(-27),
                    FinalizedDate = DateTime.Now.AddDays(-5),
                    ClaimId = "CL001",
                    ClaimIdentifier = "CI001",
                    UnderwritingPeriod = underwritingPeriod
                };
                context.TableName.Add(claim);

                var reserve1 = new Reserve
                {
                    ClaimId = "CL001",
                    ReserveType = "XYZ",
                    TransactionDate = DateTime.Now.AddDays(-26)
                };
                var reserve2 = new Reserve
                {
                    ClaimId = "CL001",
                    ReserveType = "ABC",
                    TransactionDate = DateTime.Now.AddDays(-25)
                };
                context.Reserves.AddRange(reserve1, reserve2);

                context.SaveChanges();
            }
        }

        private static void RunProblematicQuery(AppDbContext dbContext)
        {
            try
            {
                var avgGrid = dbContext.TableName.Select(z => new
                {
                    IncidentDate = z.IncidentDate,
                    ReportReceivedDate = z.ReportReceived,
                    InitiatedDate = z.InitiatedDate,
                    FinalizedDate = z.FinalizedDate,
                    FirstReserveDate = dbContext.Reserves.Where(x => x.ClaimId == z.ClaimId && x.ReserveType == "XYZ").OrderBy(x => x.TransactionDate).FirstOrDefault().TransactionDate,
                    FirstPaymentDate = dbContext.Reserves.Where(x => x.ClaimId == z.ClaimId && (x.ReserveType == "ABC" || x.ReserveType == "EFG")).OrderBy(x => x.TransactionDate).FirstOrDefault().TransactionDate,
                    ClaimIdentifier = z.ClaimIdentifier,
                    Category = z.UnderwritingPeriod == null ? "" :
                                z.UnderwritingPeriod.PolicyNumber1 != null ? "Type1" :
                                z.UnderwritingPeriod.PolicyNumber2 != null ? "Type2" :
                                z.UnderwritingPeriod.PolicyNumber3 != null ? "Type3" : "",
                    PolicyIdentifier = z.UnderwritingPeriod == null ? "" : z.UnderwritingPeriod.Policy.PolicyIdentifier,
                }).Select(z => new ClaimsReactionsReportDto
                {
                    IncidentDate = z.IncidentDate,
                    ReportReceivedDate = z.ReportReceivedDate,
                    InitiatedDate = z.InitiatedDate,
                    FinalizedDate = z.FinalizedDate,
                    FirstReserveDate = z.FirstReserveDate,
                    FirstPaymentDate = z.FirstPaymentDate,
                    IncidentToReceivedDays = (z.IncidentDate.HasValue && z.ReportReceivedDate.HasValue) ? EF.Functions.DateDiffDay(z.IncidentDate, z.ReportReceivedDate).Value : (int?)null,
                    ReceivedToInitiatedDays = (z.ReportReceivedDate.HasValue && z.InitiatedDate.HasValue) ? EF.Functions.DateDiffDay(z.ReportReceivedDate, z.InitiatedDate).Value : (int?)null,
                    InitiatedToFirstReserveDays = (z.InitiatedDate.HasValue) ? EF.Functions.DateDiffDay(z.InitiatedDate, z.FirstReserveDate).Value : (int?)null,
                    InitiatedToFirstPaymentDays = (z.InitiatedDate.HasValue) ? EF.Functions.DateDiffDay(z.InitiatedDate, z.FirstPaymentDate).Value : (int?)null,
                    InitiatedToFinalizedDays = (z.InitiatedDate.HasValue && z.FinalizedDate.HasValue) ? EF.Functions.DateDiffDay(z.InitiatedDate, z.FinalizedDate).Value : (int?)null,
                    ClaimIdentifier = z.ClaimIdentifier,
                    Category = z.Category,
                    PolicyIdentifier = z.PolicyIdentifier,
                }).GroupBy(y => new { y.IncidentDate.Value.Year, y.IncidentDate.Value.Month })
                .Select(e => new ClaimsReactionsAVGReportDto
                {
                    IncidentToReceivedDaysAVG = e.Average(x => x.IncidentToReceivedDays),
                    ReceivedToInitiatedDaysAVG = e.Average(x => x.ReceivedToInitiatedDays),
                    InitiatedToFirstReserveDaysAVG = e.Average(x => x.InitiatedToFirstReserveDays),
                    InitiatedToFirstPaymentDaysAVG = e.Average(x => x.InitiatedToFirstPaymentDays),
                    InitiatedToFinalizedDaysAVG = e.Average(x => x.InitiatedToFinalizedDays),
                    Year = e.Key.Year,
                    Month = e.Key.Month,
                    DateString = EF.Functions.DateTimeFromParts(e.Key.Year, e.Key.Month, 3, 1, 1, 1, 0),
                });

                var test = avgGrid.ToList(); // This line will throw the error
                Console.WriteLine("Query executed successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while executing the query:");
                Console.WriteLine(ex.Message);
            }
        }
    }
}
