using System.Reflection;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace Pipelines.Xml;

public interface IXmlSerializationSettings
{
    XmlSerializer Serializer { get; }

    Boolean? ConsiderElement(XmlReader reader);

    void WriteHeader(XmlWriter writer);
    void WriteFooter(XmlWriter writer);
}

class DefaultXmlSerializationSettings : IXmlSerializationSettings
{
    private readonly XName rootName;
    private readonly XmlSerializer serializer;
    private readonly XName itemName;

    public DefaultXmlSerializationSettings(Type itemType, XName rootName, XmlSerializer serializer = null)
    {
        var attribute = itemType.GetCustomAttribute<XmlTypeAttribute>();

        itemName = XName.Get(attribute.TypeName, attribute.Namespace);
        this.rootName = rootName;

        this.serializer = serializer ?? new XmlSerializer(itemType);
    }

    public XmlSerializer Serializer => serializer;

    public Boolean? ConsiderElement(XmlReader reader)
    {
        if (itemName.NamespaceName == null)
        {
            return reader.LocalName == itemName.LocalName;
        }
        else
        {
            return reader.LocalName == itemName.LocalName && reader.NamespaceURI == itemName.NamespaceName;
        }
    }

    public void WriteHeader(XmlWriter writer)
    {
        writer.WriteStartElement(rootName.LocalName, rootName.NamespaceName);
    }

    public void WriteFooter(XmlWriter writer)
    {
        writer.WriteEndElement();
    }
}

public record CustomXmlSerializationSettings(
    Type type,
    XmlSerializer Serializer = null,
    Func<XmlReader, Boolean?> considerElement = null,
    Action<XmlWriter> writeHeader = null,
    Action<XmlWriter> writeFooter = null
) : IXmlSerializationSettings
{
    public Boolean? ConsiderElement(XmlReader reader) => considerElement(reader);

    public void WriteFooter(XmlWriter writer) => writeFooter(writer);

    public void WriteHeader(XmlWriter writer) => writeHeader(writer);
}

public static partial class XmlExtensions
{
    public static IEnumerablePipeEnd<T> Xml<T>(this IStreamPipeEnd source, XName rootName = null)
        => source.Xml<T>(new DefaultXmlSerializationSettings(typeof(T), rootName ?? "items"));

    public static IEnumerablePipeEnd<T> Xml<T>(this IStreamPipeEnd source, IXmlSerializationSettings settings)
        => source.Itemize<T, IXmlSerializationSettings>(nameof(Xml), settings, ParseXml, SerializeXml);

    static void ParseXml<T>(TextReader textReader, Action<T> sink, IXmlSerializationSettings settings)
    {
        var reader = XmlReader.Create(textReader);

        reader.MoveToContent();

        var serializer = settings.Serializer;

        while (reader.Read())
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Element:
                    if (settings.ConsiderElement(reader) is not Boolean consideration) continue;

                    if (consideration)
                    {
                        var item = (T)serializer.Deserialize(reader);

                        sink(item);
                    }
                    else
                    {
                        serializer.Deserialize(reader);
                    }

                    break;
            }
        }
    }

    static void SerializeXml<T>(TextWriter textWriter, IEnumerable<T> source, IXmlSerializationSettings settings)
    {
        var writer = XmlWriter.Create(textWriter);

        var serializer = settings.Serializer;

        settings.WriteHeader(writer);

        foreach (var item in source)
        {
            serializer.Serialize(writer, item);
        }

        settings.WriteFooter(writer);
    }
}
