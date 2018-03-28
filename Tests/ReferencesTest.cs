using System;
using System.Reflection;
using NUnit.Framework;
using OdpNetMicroMapper;

namespace Tests
{
    [TestFixture]
    class ReferencesTest
    {
        [Test]
        public void PrintMyReferences()
        {
            var orm = new DbMapper();
            Console.WriteLine("Runtime Assemblies");

            var loadedAssemblies = AppDomain.CurrentDomain.GetAssemblies();
            foreach (var a in loadedAssemblies)
            {
                PortableExecutableKinds peKind;
                ImageFileMachine imageFileMachine;
                a.ManifestModule.GetPEKind(out peKind, out imageFileMachine);
                Console.WriteLine(a + " / " + a.ImageRuntimeVersion + " / " + peKind + " / " + imageFileMachine);
            }
        }
    }
}
